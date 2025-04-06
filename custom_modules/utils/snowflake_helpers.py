import os
import logging
from jinja2 import Environment, FileSystemLoader
from custom_modules.config.snowflake_config import SQL_DIR

logger = logging.getLogger(__name__)

def render_sql_template(sql_filename, context=None):
    """
    SQL 파일을 Jinja2 템플릿으로 렌더링하는 함수
    """
    env = Environment(loader=FileSystemLoader(SQL_DIR))
    template = env.get_template(sql_filename)
    rendered_sql = template.render(context or {})
    return rendered_sql
