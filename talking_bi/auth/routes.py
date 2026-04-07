import hashlib
import os
from datetime import datetime, timezone
from urllib.parse import quote_plus

import requests
from fastapi import APIRouter, Depends, HTTPException, Query, Request
from fastapi.responses import RedirectResponse
from jose import JWTError
from sqlalchemy.orm import Session

from auth.dependencies import get_current_user
from auth.models import AuthActivityLog, User, UserAPIKey
from auth.schemas import (
    APIKeyInfo,
    APIKeyUpsertRequest,
    ActivityInfo,
    ChangePasswordRequest,
    OAuthStartResponse,
    ProfileUpdateRequest,
    ResetPasswordRequest,
    TokenResponse,
    UserCreate,
    UserLogin,
    UserProfile,
)
from auth.service import (
    create_access_token,
    create_oauth_state,
    decode_oauth_state,
    hash_password,
    verify_password,
)
from database import get_db

router = APIRouter(prefix="/auth", tags=["auth"])

FRONTEND_URL = os.getenv("FRONTEND_URL", "http://127.0.0.1:5173")
GOOGLE_CLIENT_ID = os.getenv("GOOGLE_CLIENT_ID", "")
GOOGLE_CLIENT_SECRET = os.getenv("GOOGLE_CLIENT_SECRET", "")
GOOGLE_REDIRECT_URI = os.getenv("GOOGLE_REDIRECT_URI", "http://127.0.0.1:8000/auth/oauth/google/callback")
GITHUB_CLIENT_ID = os.getenv("GITHUB_CLIENT_ID", "")
GITHUB_CLIENT_SECRET = os.getenv("GITHUB_CLIENT_SECRET", "")
GITHUB_REDIRECT_URI = os.getenv("GITHUB_REDIRECT_URI", "http://127.0.0.1:8000/auth/oauth/github/callback")


def _hash_secret(secret: str) -> str:
    return hashlib.sha256(secret.encode("utf-8")).hexdigest()


def _mask_secret(secret: str) -> str:
    if len(secret) <= 8:
        return "*" * len(secret)
    return f"{secret[:4]}{'*' * (len(secret) - 8)}{secret[-4:]}"


def _touch_api_key(key: UserAPIKey) -> None:
    key.updated_at = datetime.now(timezone.utc)


def _build_profile(user: User) -> UserProfile:
    return UserProfile(
        id=user.id,
        email=user.email,
        role=user.role or "user",
        org_id=user.org_id,
        display_name=user.display_name,
        avatar_url=user.avatar_url,
    )


def _record_activity(
    db: Session,
    user_id: str,
    event_type: str,
    request: Request | None = None,
    provider: str | None = None,
) -> None:
    try:
        activity = AuthActivityLog(
            user_id=user_id,
            event_type=event_type,
            provider=provider,
            ip_address=(request.client.host if request and request.client else None),
            user_agent=(request.headers.get("user-agent") if request else None),
        )
        db.add(activity)
        db.commit()
    except Exception:
        db.rollback()


def _get_or_create_user_by_email(db: Session, email: str) -> User:
    user = db.query(User).filter(User.email == email).first()
    if user:
        return user
    generated = hash_password(os.urandom(32).hex())
    user = User(email=email, password_hash=generated)
    db.add(user)
    db.commit()
    db.refresh(user)
    return user


@router.post("/register", response_model=TokenResponse)
def register(user: UserCreate, request: Request, db: Session = Depends(get_db)):
    existing = db.query(User).filter(User.email == user.email).first()
    if existing:
        raise HTTPException(status_code=400, detail="Email already registered")

    new_user = User(email=user.email, password_hash=hash_password(user.password))
    db.add(new_user)
    db.commit()
    db.refresh(new_user)
    _record_activity(db, new_user.id, "register", request=request)

    token = create_access_token({"user_id": new_user.id})
    return {"access_token": token}


@router.post("/login", response_model=TokenResponse)
def login(user: UserLogin, request: Request, db: Session = Depends(get_db)):
    db_user = db.query(User).filter(User.email == user.email).first()
    if not db_user or not verify_password(user.password, db_user.password_hash):
        raise HTTPException(status_code=401, detail="Invalid credentials")

    _record_activity(db, db_user.id, "login", request=request)
    token = create_access_token({"user_id": db_user.id})
    return {"access_token": token}


@router.get("/me", response_model=UserProfile)
def me(current_user: User = Depends(get_current_user)):
    return _build_profile(current_user)


@router.put("/profile", response_model=UserProfile)
def update_profile(
    payload: ProfileUpdateRequest,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    if payload.display_name is not None:
        cleaned = payload.display_name.strip()
        current_user.display_name = cleaned[:120] if cleaned else None
    if payload.avatar_url is not None:
        cleaned_url = payload.avatar_url.strip()
        current_user.avatar_url = cleaned_url[:2048] if cleaned_url else None

    db.add(current_user)
    db.commit()
    db.refresh(current_user)
    return _build_profile(current_user)


@router.post("/change-password")
def change_password(
    payload: ChangePasswordRequest,
    request: Request,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    if not verify_password(payload.current_password, current_user.password_hash):
        raise HTTPException(status_code=400, detail="Current password is incorrect")
    if len(payload.new_password) < 8:
        raise HTTPException(status_code=400, detail="New password must be at least 8 characters")
    current_user.password_hash = hash_password(payload.new_password)
    db.add(current_user)
    db.commit()
    _record_activity(db, current_user.id, "password_change", request=request)
    return {"status": "ok", "message": "Password updated successfully"}


@router.post("/reset-password")
def reset_password(payload: ResetPasswordRequest, db: Session = Depends(get_db)):
    return {"status": "ok", "message": "If this email exists, a reset link has been sent"}


@router.get("/api-keys", response_model=list[APIKeyInfo])
def list_api_keys(
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    keys = (
        db.query(UserAPIKey)
        .filter(UserAPIKey.user_id == current_user.id)
        .order_by(UserAPIKey.updated_at.desc())
        .all()
    )
    return [
        APIKeyInfo(
            id=k.id,
            provider=k.provider,
            label=k.label,
            secret_masked=k.secret_masked,
            created_at=k.created_at.isoformat() if k.created_at else "",
            updated_at=k.updated_at.isoformat() if k.updated_at else "",
        )
        for k in keys
    ]


@router.post("/api-keys", response_model=APIKeyInfo)
def upsert_api_key(
    payload: APIKeyUpsertRequest,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    provider = payload.provider.strip().lower()
    if not provider:
        raise HTTPException(status_code=400, detail="Provider is required")
    secret = payload.secret.strip()
    if not secret:
        raise HTTPException(status_code=400, detail="Secret is required")

    row = (
        db.query(UserAPIKey)
        .filter(UserAPIKey.user_id == current_user.id, UserAPIKey.provider == provider)
        .first()
    )
    if row is None:
        row = UserAPIKey(
            user_id=current_user.id,
            provider=provider,
            label=(payload.label or provider).strip()[:120],
            secret_masked=_mask_secret(secret),
            secret_hash=_hash_secret(secret),
        )
    else:
        row.label = (payload.label or row.label or provider).strip()[:120]
        row.secret_masked = _mask_secret(secret)
        row.secret_hash = _hash_secret(secret)
        _touch_api_key(row)

    db.add(row)
    db.commit()
    db.refresh(row)
    return APIKeyInfo(
        id=row.id,
        provider=row.provider,
        label=row.label,
        secret_masked=row.secret_masked,
        created_at=row.created_at.isoformat() if row.created_at else "",
        updated_at=row.updated_at.isoformat() if row.updated_at else "",
    )


@router.delete("/api-keys/{key_id}")
def delete_api_key(
    key_id: str,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    row = (
        db.query(UserAPIKey)
        .filter(UserAPIKey.id == key_id, UserAPIKey.user_id == current_user.id)
        .first()
    )
    if row is None:
        raise HTTPException(status_code=404, detail="API key entry not found")
    db.delete(row)
    db.commit()
    return {"status": "ok"}


@router.get("/activity", response_model=list[ActivityInfo])
def activity(
    limit: int = Query(default=20, ge=1, le=100),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    rows = (
        db.query(AuthActivityLog)
        .filter(AuthActivityLog.user_id == current_user.id)
        .order_by(AuthActivityLog.created_at.desc())
        .limit(limit)
        .all()
    )
    return [
        ActivityInfo(
            id=r.id,
            event_type=r.event_type,
            provider=r.provider,
            ip_address=r.ip_address,
            user_agent=r.user_agent,
            created_at=r.created_at.isoformat() if r.created_at else "",
        )
        for r in rows
    ]


@router.get("/oauth/{provider}/start", response_class=RedirectResponse)
def oauth_start(
    provider: str,
    redirect_uri: str | None = Query(default=None, description="Frontend redirect path after auth"),
):
    provider = provider.lower().strip()
    frontend_redirect = redirect_uri or f"{FRONTEND_URL}/auth/callback"

    if provider == "google":
        if not GOOGLE_CLIENT_ID or not GOOGLE_CLIENT_SECRET:
            raise HTTPException(status_code=400, detail="Google OAuth is not configured")
        state = create_oauth_state({"provider": "google", "frontend_redirect": frontend_redirect})
        auth_url = (
            "https://accounts.google.com/o/oauth2/v2/auth"
            f"?client_id={quote_plus(GOOGLE_CLIENT_ID)}"
            f"&redirect_uri={quote_plus(GOOGLE_REDIRECT_URI)}"
            "&response_type=code"
            "&scope=openid%20email%20profile"
            f"&state={quote_plus(state)}"
            "&access_type=offline&prompt=consent"
        )
        return RedirectResponse(url=auth_url, status_code=302)

    if provider == "github":
        if not GITHUB_CLIENT_ID or not GITHUB_CLIENT_SECRET:
            raise HTTPException(status_code=400, detail="GitHub OAuth is not configured")
        state = create_oauth_state({"provider": "github", "frontend_redirect": frontend_redirect})
        auth_url = (
            "https://github.com/login/oauth/authorize"
            f"?client_id={quote_plus(GITHUB_CLIENT_ID)}"
            f"&redirect_uri={quote_plus(GITHUB_REDIRECT_URI)}"
            "&scope=read:user%20user:email"
            f"&state={quote_plus(state)}"
        )
        return RedirectResponse(url=auth_url, status_code=302)

    raise HTTPException(status_code=404, detail="Unsupported OAuth provider")


@router.get("/oauth/{provider}/url", response_model=OAuthStartResponse)
def oauth_start_url(
    provider: str,
    redirect_uri: str | None = Query(default=None, description="Frontend redirect path after auth"),
):
    provider = provider.lower().strip()
    frontend_redirect = redirect_uri or f"{FRONTEND_URL}/auth/callback"
    state = create_oauth_state({"provider": provider, "frontend_redirect": frontend_redirect})

    if provider == "google":
        if not GOOGLE_CLIENT_ID or not GOOGLE_CLIENT_SECRET:
            raise HTTPException(status_code=400, detail="Google OAuth is not configured")
        auth_url = (
            "https://accounts.google.com/o/oauth2/v2/auth"
            f"?client_id={quote_plus(GOOGLE_CLIENT_ID)}"
            f"&redirect_uri={quote_plus(GOOGLE_REDIRECT_URI)}"
            "&response_type=code"
            "&scope=openid%20email%20profile"
            f"&state={quote_plus(state)}"
            "&access_type=offline&prompt=consent"
        )
        return {"provider": provider, "auth_url": auth_url}

    if provider == "github":
        if not GITHUB_CLIENT_ID or not GITHUB_CLIENT_SECRET:
            raise HTTPException(status_code=400, detail="GitHub OAuth is not configured")
        auth_url = (
            "https://github.com/login/oauth/authorize"
            f"?client_id={quote_plus(GITHUB_CLIENT_ID)}"
            f"&redirect_uri={quote_plus(GITHUB_REDIRECT_URI)}"
            "&scope=read:user%20user:email"
            f"&state={quote_plus(state)}"
        )
        return {"provider": provider, "auth_url": auth_url}

    raise HTTPException(status_code=404, detail="Unsupported OAuth provider")


@router.get("/oauth/google/callback", response_class=RedirectResponse)
def oauth_google_callback(code: str, state: str, request: Request, db: Session = Depends(get_db)):
    try:
        state_payload = decode_oauth_state(state)
    except (JWTError, ValueError):
        raise HTTPException(status_code=400, detail="Invalid OAuth state")

    token_resp = requests.post(
        "https://oauth2.googleapis.com/token",
        data={
            "code": code,
            "client_id": GOOGLE_CLIENT_ID,
            "client_secret": GOOGLE_CLIENT_SECRET,
            "redirect_uri": GOOGLE_REDIRECT_URI,
            "grant_type": "authorization_code",
        },
        timeout=20,
    )
    if token_resp.status_code >= 400:
        raise HTTPException(status_code=400, detail="Google token exchange failed")
    access_token = token_resp.json().get("access_token")
    if not access_token:
        raise HTTPException(status_code=400, detail="Google access token missing")

    profile_resp = requests.get(
        "https://www.googleapis.com/oauth2/v3/userinfo",
        headers={"Authorization": f"Bearer {access_token}"},
        timeout=20,
    )
    if profile_resp.status_code >= 400:
        raise HTTPException(status_code=400, detail="Google profile fetch failed")
    payload = profile_resp.json()
    email = payload.get("email")
    if not email:
        raise HTTPException(status_code=400, detail="Google account email not available")

    user = _get_or_create_user_by_email(db, email)
    if not user.display_name and payload.get("name"):
        user.display_name = str(payload["name"])[:120]
        db.add(user)
        db.commit()
        db.refresh(user)
    _record_activity(db, user.id, "oauth_login", request=request, provider="google")

    app_token = create_access_token({"user_id": user.id})
    frontend_redirect = state_payload.get("frontend_redirect") or f"{FRONTEND_URL}/auth/callback"
    return RedirectResponse(url=f"{frontend_redirect}?token={quote_plus(app_token)}&provider=google", status_code=302)


@router.get("/oauth/github/callback", response_class=RedirectResponse)
def oauth_github_callback(code: str, state: str, request: Request, db: Session = Depends(get_db)):
    try:
        state_payload = decode_oauth_state(state)
    except (JWTError, ValueError):
        raise HTTPException(status_code=400, detail="Invalid OAuth state")

    token_resp = requests.post(
        "https://github.com/login/oauth/access_token",
        data={
            "client_id": GITHUB_CLIENT_ID,
            "client_secret": GITHUB_CLIENT_SECRET,
            "code": code,
            "redirect_uri": GITHUB_REDIRECT_URI,
            "state": state,
        },
        headers={"Accept": "application/json"},
        timeout=20,
    )
    if token_resp.status_code >= 400:
        raise HTTPException(status_code=400, detail="GitHub token exchange failed")
    gh_access_token = token_resp.json().get("access_token")
    if not gh_access_token:
        raise HTTPException(status_code=400, detail="GitHub access token missing")

    user_resp = requests.get(
        "https://api.github.com/user",
        headers={"Authorization": f"Bearer {gh_access_token}", "Accept": "application/vnd.github+json"},
        timeout=20,
    )
    if user_resp.status_code >= 400:
        raise HTTPException(status_code=400, detail="GitHub profile fetch failed")
    user_payload = user_resp.json()
    email = user_payload.get("email")

    if not email:
        emails_resp = requests.get(
            "https://api.github.com/user/emails",
            headers={"Authorization": f"Bearer {gh_access_token}", "Accept": "application/vnd.github+json"},
            timeout=20,
        )
        if emails_resp.status_code < 400:
            emails = emails_resp.json() or []
            primary = next((e for e in emails if e.get("primary") and e.get("verified")), None)
            fallback = next((e for e in emails if e.get("verified")), None)
            email = (primary or fallback or {}).get("email")

    if not email:
        raise HTTPException(status_code=400, detail="GitHub account email not available")

    user = _get_or_create_user_by_email(db, email)
    if not user.display_name:
        display_name = user_payload.get("name") or user_payload.get("login")
        if display_name:
            user.display_name = str(display_name)[:120]
            db.add(user)
            db.commit()
            db.refresh(user)
    _record_activity(db, user.id, "oauth_login", request=request, provider="github")

    app_token = create_access_token({"user_id": user.id})
    frontend_redirect = state_payload.get("frontend_redirect") or f"{FRONTEND_URL}/auth/callback"
    return RedirectResponse(url=f"{frontend_redirect}?token={quote_plus(app_token)}&provider=github", status_code=302)
