"""Run only a loopback-bound local server; never a Cloud Run entry point."""

if __name__ == "__main__":
    from devtools.config import DevConfig

    DevConfig.from_environment()
    import uvicorn

    uvicorn.run(
        "devtools.app:create_app", factory=True, host="127.0.0.1", port=8081, access_log=False
    )
