from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from database import get_db
from auth.models import User
from auth.schemas import UserCreate, UserLogin, TokenResponse
from auth.service import hash_password, verify_password, create_access_token

router = APIRouter(prefix="/auth", tags=["auth"])

@router.post("/register", response_model=TokenResponse)
def register(user: UserCreate, db: Session = Depends(get_db)):
    existing = db.query(User).filter(User.email == user.email).first()

    if existing:
        raise HTTPException(status_code=400, detail="Email already registered")

    new_user = User(
        email=user.email,
        password_hash=hash_password(user.password)
    )

    try:
        db.add(new_user)
        db.commit()
        db.refresh(new_user)

        token = create_access_token({"user_id": new_user.id})

        return {"access_token": token}
    except Exception as e:
        print(f"DEBUG REGISTER ERROR: {str(e)}")
        raise e

@router.post("/login", response_model=TokenResponse)
def login(user: UserLogin, db: Session = Depends(get_db)):
    db_user = db.query(User).filter(User.email == user.email).first()

    if not db_user or not verify_password(user.password, db_user.password_hash):
        raise HTTPException(status_code=401, detail="Invalid credentials")

    token = create_access_token({"user_id": db_user.id})

    return {"access_token": token}
