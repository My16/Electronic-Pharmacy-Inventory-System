# routers.py
# Keeps iHOMIS queries on the 'ihomis' database connection.
# All other models stay on 'default'.


IHOMIS_APP_LABEL  = 'ihomis'          # used if you put HPerson in its own app
IHOMIS_DB_ALIAS   = 'ihomis_plus'     # matches the key in settings.DATABASES


class IHOMISRouter:
    """
    Route all queries for HPerson (and any future iHOMIS models) to the
    read-only 'ihomis' database.  Everything else goes to 'default'.
    """

    # The db_table names that belong to iHOMIS — add more if needed later.
    IHOMIS_TABLES = {
        'hperson',
        # 'hadmission', 'horder', ...   ← add as you expand
    }

    def _is_ihomis(self, model):
        return getattr(model._meta, 'db_table', '') in self.IHOMIS_TABLES

    # ── Read ──────────────────────────────────────────────────
    def db_for_read(self, model, **hints):
        if self._is_ihomis(model):
            return IHOMIS_DB_ALIAS
        return None   # let Django use 'default'

    # ── Write — always block ──────────────────────────────────
    def db_for_write(self, model, **hints):
        if self._is_ihomis(model):
            return None   # raise an error on any write attempt
        return None

    # ── Migrations — never touch iHOMIS tables ────────────────
    def allow_migrate(self, db, app_label, model_name=None, **hints):
        if db == IHOMIS_DB_ALIAS:
            return False   # never migrate to iHOMIS DB
        return None        # let default rules handle everything else

    # ── Cross-DB relations — not allowed ─────────────────────
    def allow_relation(self, obj1, obj2, **hints):
        # Allow relations only within the same database
        if self._is_ihomis(obj1) or self._is_ihomis(obj2):
            return False
        return None