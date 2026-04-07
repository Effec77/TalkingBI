from pydantic import BaseModel, EmailStr

class UserCreate(BaseModel):
    email: EmailStr
    password: str

class UserLogin(BaseModel):
    email: EmailStr
    password: str

class TokenResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"


class UserProfile(BaseModel):
    id: str
    email: EmailStr
    role: str
    org_id: str | None = None
    display_name: str | None = None
    avatar_url: str | None = None


class ChangePasswordRequest(BaseModel):
    current_password: str
    new_password: str


class ResetPasswordRequest(BaseModel):
    email: EmailStr


class OAuthStartResponse(BaseModel):
    provider: str
    auth_url: str


class ProfileUpdateRequest(BaseModel):
    display_name: str | None = None
    avatar_url: str | None = None


class APIKeyUpsertRequest(BaseModel):
    provider: str
    label: str | None = None
    secret: str


class APIKeyInfo(BaseModel):
    id: str
    provider: str
    label: str | None = None
    secret_masked: str
    created_at: str
    updated_at: str


class ActivityInfo(BaseModel):
    id: str
    event_type: str
    provider: str | None = None
    ip_address: str | None = None
    user_agent: str | None = None
    created_at: str
