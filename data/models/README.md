# Model Artifacts

This directory is reserved for model weights, configs, and metadata used by ML processing steps. Currently empty — models are loaded from HuggingFace cache or called via API.

## Current model usage

| Step | Model | Source | Storage |
|---|---|---|---|
| Emotion classification (`emotions.py`) | `SamLowe/roberta-base-go_emotions` | HuggingFace Hub | `~/.cache/huggingface/hub/` |
| Emotion classification (`emotions.py`) | `j-hartmann/emotion-english-distilroberta-base` | HuggingFace Hub | `~/.cache/huggingface/hub/` |
| Emotion classification (`emotions.py`) | `cardiffnlp/twitter-roberta-base-sentiment-latest` | HuggingFace Hub | `~/.cache/huggingface/hub/` |
| Sentiment (`sentiment.py`) | VADER | `vaderSentiment` pip package | Installed package |
| Sentiment (`sentiment.py`) | NRCLex | NLTK corpora | `~/nltk_data/` |
| Audit Tier B/C (`audit.py`) | `google/gemini-2.0-flash-001` | OpenRouter API | No local storage |
| Reddit extraction (`extract_reddit.py`) | `google/gemini-2.0-flash-001` | OpenRouter API | No local storage |

## Adding a new model

If you add a model that requires local weights (e.g., a fine-tuned classifier):

1. Place weights in `data/models/<model_name>/`
2. Add a `.gitignore` entry for the weights (they're too large for git)
3. Document the model in this README with download instructions
4. In your code, use `MODEL_DIR = os.path.join(os.path.dirname(__file__), "data", "models", "<model_name>")`
