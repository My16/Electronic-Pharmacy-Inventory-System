# context_processors.py
from .models import UserPermission
from .views import can_access_module

class PermProxy:
    """Allows {{ perm.inventory }} style access in templates."""
    def __init__(self, user):
        self._user = user

    def __getattr__(self, module):
        return can_access_module(self._user, module)

def module_permissions(request):
    if not request.user.is_authenticated:
        return {'perm': PermProxy.__new__(PermProxy)}  # empty proxy
    return {'perm': PermProxy(request.user)}