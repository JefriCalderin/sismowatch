"""
Módulo de Seguridad, Autenticación JWT y Control de Acceso Basado en Roles (RBAC).
Implementa validación de tokens y protección estricta contra elevación de privilegios.
"""
from datetime import datetime, timedelta, timezone
from typing import Optional, List, Dict, Any
import hashlib
import hmac
from jose import JWTError, jwt
from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from api.app.config import api_settings

security_bearer = HTTPBearer(auto_error=False)

def verify_credentials(plain_password: str, expected_password: str) -> bool:
    """Verificación de contraseñas resistente a ataques de temporización."""
    return hmac.compare_digest(plain_password.encode("utf-8"), expected_password.encode("utf-8"))

def authenticate_user(username: str, password: str) -> Optional[Dict[str, Any]]:
    """Autentica al usuario contra las credenciales inyectadas por variables de entorno."""
    if username == api_settings.ADMIN_USERNAME and verify_credentials(password, api_settings.ADMIN_PASSWORD):
        return {"username": api_settings.ADMIN_USERNAME, "role": "admin", "full_name": "Administrador SismoWatch"}
    elif username == api_settings.VIEWER_USERNAME and verify_credentials(password, api_settings.VIEWER_PASSWORD):
        return {"username": api_settings.VIEWER_USERNAME, "role": "viewer", "full_name": "Observador de Operaciones"}
    return None

def create_access_token(data: dict, expires_delta: Optional[timedelta] = None) -> str:
    """Genera un token JWT firmado con claims de rol y expiración."""
    to_encode = data.copy()
    expire = datetime.now(timezone.utc) + (expires_delta or timedelta(minutes=api_settings.ACCESS_TOKEN_EXPIRE_MINUTES))
    to_encode.update({"exp": expire, "iat": datetime.now(timezone.utc)})
    return jwt.encode(to_encode, api_settings.JWT_SECRET, algorithm=api_settings.JWT_ALGORITHM)

def get_current_user(credentials: Optional[HTTPAuthorizationCredentials] = Depends(security_bearer)) -> Dict[str, Any]:
    """
    Dependencia de seguridad: Valida el token JWT en el encabezado Authorization.
    Si no hay token o es inválido, rechaza con HTTP 401 Unauthorized.
    """
    if not credentials or not credentials.credentials:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Acceso no autorizado: Se requiere token de autenticación Bearer válido.",
            headers={"WWW-Authenticate": "Bearer"},
        )
    token = credentials.credentials
    try:
        payload = jwt.decode(token, api_settings.JWT_SECRET, algorithms=[api_settings.JWT_ALGORITHM])
        username: str = payload.get("sub")
        role: str = payload.get("role")
        if not username or not role:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Token no contiene los claims requeridos de identidad y rol.",
                headers={"WWW-Authenticate": "Bearer"},
            )
        return {"username": username, "role": role}
    except JWTError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token de acceso inválido o expirado.",
            headers={"WWW-Authenticate": "Bearer"},
        )

def require_role(allowed_roles: List[str]):
    """
    Comprueba que el rol del usuario esté dentro de los roles autorizados.
    Si un 'viewer' intenta ejecutar una acción reservada para 'admin', devuelve HTTP 403 Forbidden.
    """
    def role_checker(current_user: Dict[str, Any] = Depends(get_current_user)) -> Dict[str, Any]:
        user_role = current_user.get("role")
        if user_role not in allowed_roles:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"Permiso denegado: El rol '{user_role}' no está autorizado para realizar esta acción. Requiere: {allowed_roles}."
            )
        return current_user
    return role_checker
