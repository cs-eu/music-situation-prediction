import wandb
import os

def setup_wandb(project_name, config, run_name=None):
    """Setup Weights and Biases logging."""
    wandb.init(
        project=project_name,
        name=run_name,
        config=config,
        dir=os.path.join(os.getcwd(), "outputs", "logs")
    )

def log_metrics(metrics, step=None):
    """Log metrics to Weights and Biases."""
    wandb.log(metrics, step=step)

def log_artifact(artifact_name, artifact_type, file_path):
    """Log an artifact to Weights and Biases."""
    artifact = wandb.Artifact(artifact_name, type=artifact_type)
    artifact.add_file(file_path)
    wandb.log_artifact(artifact)