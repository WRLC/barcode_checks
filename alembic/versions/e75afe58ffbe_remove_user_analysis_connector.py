"""Remove user_analysis_connector

Revision ID: e75afe58ffbe
Revises: 18ae93446498
Create Date: 2025-04-17 12:04:13.789372

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import mysql

# revision identifiers, used by Alembic.
revision: str = 'e75afe58ffbe'
down_revision: Union[str, None] = '18ae93446498'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    # ### commands auto generated by Alembic - please adjust! ###
    # op.drop_index('uq_user_analysis', table_name='user_analysis_connector')
    op.drop_table('user_analysis_connector')
    # ### end Alembic commands ###


def downgrade() -> None:
    """Downgrade schema."""
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table('user_analysis_connector',
    sa.Column('id', mysql.INTEGER(), autoincrement=True, nullable=False),
    sa.Column('user_id', mysql.INTEGER(), autoincrement=False, nullable=False),
    sa.Column('analysis_id', mysql.INTEGER(), autoincrement=False, nullable=False),
    sa.Column('created_at', mysql.DATETIME(), server_default=sa.text('(now())'), nullable=False),
    sa.ForeignKeyConstraint(['analysis_id'], ['analyses.id'], name='user_analysis_connector_ibfk_1'),
    sa.ForeignKeyConstraint(['user_id'], ['users.id'], name='user_analysis_connector_ibfk_2'),
    sa.PrimaryKeyConstraint('id'),
    mysql_collate='utf8mb4_0900_ai_ci',
    mysql_default_charset='utf8mb4',
    mysql_engine='InnoDB'
    )
    op.create_index('uq_user_analysis', 'user_analysis_connector', ['user_id', 'analysis_id'], unique=True)
    # ### end Alembic commands ###
