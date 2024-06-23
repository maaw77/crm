from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import SecretStr

from pathlib import Path
# Defining the paths.
BASE_DIR = Path(__file__).resolve().parent
# print(BASE_DIR/'.env')


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=BASE_DIR/'.env', env_file_encoding='utf-8')
    POSTGRES_DB: str
    POSTGRES_USER: str
    POSTGRES_PASSWORD: SecretStr
    HOST_DB: str
    PORT_DB: int
    crm_username: str
    crm_password: SecretStr


if __name__ == '__main__':
    print(Settings().crm_password.get_secret_value())

