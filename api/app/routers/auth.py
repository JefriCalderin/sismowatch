"""
Router de Autenticación de SismoWatch.
Provee endpoint de inicio de sesión con emisión de token JWT.
"""
from fastapi import APIRouter, HTTPException, status
from api.app.models import LoginRequest, LoginResponse
from api.app.security import authenticate_user, create_access_token

router = APIRouter(prefix="/auth", tags=["Autenticación"])

@router.post("/login", response_model=LoginResponse)
def login(creds: LoginRequest):
    """
    Inicia sesión en la plataforma y entrega un token JWT con el rol correspondiente (admin o viewer).
    """
    user = authenticate_user(creds.username, creds.password)
    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Credenciales inválidas. Verifique usuario y contraseña.",
            headers={"WWW-Authenticate": "Bearer"},
        )
    
    token = create_access_token(data={"sub": user["username"], "role": user["role"]})
    return LoginResponse(
        access_token=token,
        token_type="bearer",
        role=user["role"],
        username=user["username"]
    )
