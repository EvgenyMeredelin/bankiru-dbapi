import logfire
import uvicorn
from environs import env


env.read_env(recurse=True)
logfire.configure(token=env("LOGFIRE_TOKEN"), service_name="API")
logfire.install_auto_tracing(
    modules=["main", "handlers"],
    min_duration=0
)


if __name__ == "__main__":
    uvicorn.run(
        "main:app",
        host=env("ECS_PRIVATE_IP"),
        port=env.int("ECS_PORT")
    )
