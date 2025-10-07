FEATURE_FLAGS = {
    "ENABLE_TEMPLATE_PROCESSING": True,
    "ALERT_REPORTS": True
}

ENABLE_UI_THEME_ADMINISTRATION = True

ENABLE_PROXY_FIX = True
SECRET_KEY = "666!!!777@@@AaBbBc"

APP_NAME = "PPS Dashboard"

APP_ICON = "/static/assets/images/pps_logo.png"
LOGO_TARGET_PATH = "/"
LOGO_TOOLTIP = "PPS"
LOGO_RIGHT_TEXT = "PPS"

# For PROD scenario, it's better to connect to a separate databae 
#SQLALCHEMY_DATABASE_URI = "URI in SQLAlchemy style"