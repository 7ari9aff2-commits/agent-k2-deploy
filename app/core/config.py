from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    APP_NAME: str = "Agent K2 Core Engine"
    DEBUG: bool = False
    PORT: int = 8000
    HOST: str = "0.0.0.0"

    # PostgreSQL (Supabase project: iqemryazzthjgztregjx / Meruna's clinics / eu-west-1)
    DATABASE_URL: str = ""
    DB_SSL: bool = True
    DB_POOL_MIN: int = 2
    DB_POOL_MAX: int = 10

    # Channel adapters (app/channels) — Supabase REST for the patient/conversation RPCs,
    # same functions the n8n routers called (find_or_create_patient / get_or_create_channel_conversation)
    SUPABASE_REST_URL: str = "https://iqemryazzthjgztregjx.supabase.co/rest/v1"
    SUPABASE_SECRET_KEY: str = ""

    # Owner alerting bridge (FastAPI failures → owner Telegram) — the n8n Error
    # Monitor cannot see FastAPI exceptions, so the migrated path alerts directly.
    TELEGRAM_ALERT_BOT_TOKEN: str = ""
    TELEGRAM_ALERT_CHAT_ID: str = ""

    # Deferred-batch worker (in-process port of the n8n Deferred Message Worker):
    # replay deferred bursts through the core, deliver via the n8n dispatcher.
    DEFERRED_WORKER_ENABLED: bool = True
    OUTBOUND_DISPATCHER_URL: str = "https://n8n-production-33955.up.railway.app/webhook/inbox-outbound"
    OUTBOUND_DISPATCHER_TOKEN: str = ""
    SUPABASE_SECRET_KEY: str = ""

    # Webhook auth — same header contract as the n8n webhook (headerAuth, X-K2-Internal-Token)
    K2_INTERNAL_TOKEN: str = ""

    # Primary dialogue model (n8n node: DeepSeek Model, credential: GLM 5.3 FLASH)
    LLM_PRIMARY_BASE_URL: str = "https://ai-gateway.vercel.sh/v1"
    LLM_PRIMARY_API_KEY: str = ""
    LLM_PRIMARY_MODEL: str = "deepseek/deepseek-v4-flash-0731"
    LLM_TIMEOUT_SECONDS: float = 60.0
    LLM_TOOL_MAX_TURNS: int = 3
    LLM_TOOL_MAX_CALLS: int = 4

    # Reasoning control. The n8n workflow sent {"reasoning": {"enabled": false}} because its
    # DeepSeek node understood it. Reasoning models served by other gateways (e.g. Novita's
    # zai-org/glm-5.3-flash) IGNORE that flag: they still spend the completion budget on
    # hidden reasoning, and with a small max_tokens the visible content comes back EMPTY
    # (finish_reason=length). So the parameter is opt-in, and token budgets must cover
    # reasoning + output.
    LLM_SEND_REASONING_PARAM: bool = False
    LLM_REASONING_MAX_TOKENS: int = 2048
    # No flag disables thinking on zai-org/glm-5.3-flash, but reasoning_effort does throttle
    # it: measured on Novita, "low" cut reasoning tokens from 283 to 83 and call latency
    # from 7.7s to 5.4s. Empty string omits the parameter entirely.
    LLM_REASONING_EFFORT: str = "low"

    # Final response composer: model-authored prose from an authoritative fact catalog.
    LLM_COMPOSER_ENABLED: bool = True
    LLM_COMPOSER_TEMPERATURE: float = 0.35
    # Must exceed the model's hidden reasoning budget plus the reply, or content returns empty.
    LLM_COMPOSER_MAX_TOKENS: int = 2000
    LLM_COMPOSER_MAX_ATTEMPTS: int = 2

    # Repair chain model (n8n node: DeepSeek Repair Model, credential: OpenAI account 2)
    LLM_REPAIR_BASE_URL: str = "https://api.openai.com/v1"
    LLM_REPAIR_API_KEY: str = ""
    LLM_REPAIR_MODEL: str = "deepseek/deepseek-v4-flash-0731"

    # Grounding verifier (docs/agent_upgrade_design.md P1.1): enforce | audit | off


settings = Settings()
