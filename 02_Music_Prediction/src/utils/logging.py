import os


def setup_wandb(project_name, config, run_name=None):
    """Setup Weights and Biases logging."""
    settings = config.get("wandb", {})
    if settings.get("mode", "disabled") == "disabled":
        return None
    import wandb

    log_dir = os.path.join(os.getcwd(), "outputs", "logs")
    os.makedirs(log_dir, exist_ok=True)
    return wandb.init(
        project=project_name,
        name=run_name,
        config=config,
        dir=log_dir,
        mode=settings.get("mode", "offline"),
    )

def log_metrics(metrics, step=None):
    """Log metrics to Weights and Biases."""
    import wandb

    if wandb.run is not None:
        wandb.log(metrics, step=step)

def log_artifact(artifact_name, artifact_type, file_path):
    """Log an artifact to Weights and Biases."""
    import wandb

    if wandb.run is None:
        return
    artifact = wandb.Artifact(artifact_name, type=artifact_type)
    artifact.add_file(file_path)
    wandb.log_artifact(artifact)
