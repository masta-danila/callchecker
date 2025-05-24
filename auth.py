import base64
import copy
import hmac
import json
import os
from time import time
from dotenv import load_dotenv

# Загрузка переменных из файла .env
load_dotenv()

TEN_MINUTES = 600  # seconds


def generate_jwt(payload, expiration_time=TEN_MINUTES):
    """
    Генерирует JWT, используя API ключ и секретный ключ из переменных окружения.
    """
    api_key = os.getenv("API_KEY")
    secret_key = os.getenv("SECRET_KEY")

    if not api_key or not secret_key:
        raise ValueError("API_KEY или SECRET_KEY не найдены в переменных окружения")

    header = {
        "alg": "HS256",
        "typ": "JWT",
        "kid": api_key
    }
    payload_copy = copy.deepcopy(payload)
    current_timestamp = int(time())
    payload_copy["exp"] = current_timestamp + expiration_time

    payload_bytes = json.dumps(payload_copy, separators=(',', ':')).encode("utf-8")
    header_bytes = json.dumps(header, separators=(',', ':')).encode("utf-8")

    data = (base64.urlsafe_b64encode(header_bytes).strip(b'=') + b"." +
            base64.urlsafe_b64encode(payload_bytes).strip(b'='))

    signature = hmac.new(base64.urlsafe_b64decode(secret_key), msg=data, digestmod="sha256")
    jwt = data + b"." + base64.urlsafe_b64encode(signature.digest()).strip(b'=')
    return jwt.decode("utf-8")


def generate_auth_metadata(scope, type=list):
    """
    Создаёт метаданные для авторизации с использованием JWT.
    """
    auth_payload = {
        "iss": "test_issuer",
        "sub": "test_user",
        "aud": scope
    }

    metadata = [
        ("authorization", "Bearer " + generate_jwt(auth_payload))
    ]
    return type(metadata)