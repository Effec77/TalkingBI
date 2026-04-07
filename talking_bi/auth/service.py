from jose import jwt
from datetime import datetime, timedelta

import os
SECRET_KEY = os.getenv("SECRET_KEY", "supersecretkey")
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 60 * 24
OAUTH_STATE_EXPIRE_MINUTES = 10

import bcrypt

def hash_password(password: str):
    # Direct bcrypt calls avoid passlib compatibility issues with newer versions of bcrypt
    pwd_bytes = password.encode('utf-8')
    salt = bcrypt.gensalt()
    return bcrypt.hashpw(pwd_bytes, salt).decode('utf-8')

def verify_password(password: str, hashed: str):
    return bcrypt.checkpw(password.encode('utf-8'), hashed.encode('utf-8'))

def create_access_token(data: dict):
    to_encode = data.copy()
    expire = datetime.utcnow() + timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    to_encode.update({"exp": expire})
    return jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)


def create_oauth_state(data: dict):
    to_encode = data.copy()
    expire = datetime.utcnow() + timedelta(minutes=OAUTH_STATE_EXPIRE_MINUTES)
    to_encode.update({"exp": expire, "type": "oauth_state"})
    return jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)


def decode_oauth_state(state_token: str):
    payload = jwt.decode(state_token, SECRET_KEY, algorithms=[ALGORITHM])
    if payload.get("type") != "oauth_state":
        raise ValueError("Invalid state token type")
    return payload
