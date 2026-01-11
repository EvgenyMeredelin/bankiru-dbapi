import logfire
import uvicorn
from environs import env
env.read_env()


logfire.configure(service_name="API")
logfire.install_auto_tracing(
    modules=["main", "handlers"],
    min_duration=0
)


if __name__ == "__main__":
    uvicorn.run(
        app="main:app",
        host=env("ECS_PRIVATE_IP"),
        port=env.int("ECS_PORT")
    )
