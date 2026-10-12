from .permissions import can, is_admin


class PermProxy:
    """
    {{ perm.inventory }}      -> can the user open the module?   (view level)
    {{ perm_edit.inventory }} -> can the user add/edit in it?    (edit level)
    """
    def __init__(self, user=None, level='view'):
        self._user  = user
        self._level = level

    def __getattr__(self, module):
        if module.startswith('_'):
            raise AttributeError(module)
        if self._user is None:
            return False
        return can(self._user, module, self._level)


def module_permissions(request):
    user = getattr(request, 'user', None)
    if user is None or not user.is_authenticated:
        return {
            'perm':          PermProxy(),
            'perm_edit':     PermProxy(level='edit'),
            'is_admin_user': False,
        }
    return {
        'perm':          PermProxy(user),
        'perm_edit':     PermProxy(user, 'edit'),
        'is_admin_user': is_admin(user),
    }