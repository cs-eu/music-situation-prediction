import matplotlib as plt
import seaborn as sns
import wandb

def plot_attention(attention_weights, title="Attention Weights"):
    """Plot attention weights for transformer."""
    plt.figure(figsize=(10, 6))
    sns.heatmap(attention_weights, cmap="viridis")
    plt.title(title)
    wandb.log({f"{title}": wandb.Image(plt)})
    plt.close()

def plot_shap(shap_values, features, title="SHAP values"):
    """Plot SHAP values for feature importance."""
    plt.figure(figsize=(10, 6))
    sns.barplot(x=shap_values, y=features)
    plt.title(title)
    wandb.log({f"{title}": wandb.Image(plt)})
    plt.close()