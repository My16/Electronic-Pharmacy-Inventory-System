from functools import wraps

from django.contrib import messages
from django.http import JsonResponse
from django.shortcuts import redirect

ROLE_RANK  = {'user': 1, 'staff': 2, 'admin': 3}
LEVEL_RANK = {'view': 1, 'edit': 2, 'admin': 3}


def get_role(user):
    if user.is_superuser:
        return 'admin'
    try:
        return user.profile.role or 'user'
    except AttributeError:
        return 'user'


def is_admin(user):
    return get_role(user) == 'admin'


def can(user, modules=None, level='view'):
    """
    modules: None, a module key, or a tuple of keys (any one is enough).
    level:   'view' (any role), 'edit' (staff+), 'admin' (admin only).
    Admins always pass.
    """
    role = get_role(user)
    if role == 'admin':
        return True
    if ROLE_RANK.get(role, 1) < LEVEL_RANK[level]:
        return False
    if modules is None:
        return True
    if isinstance(modules, str):
        modules = (modules,)
    perms = getattr(user, 'permissions', None)
    return any(getattr(perms, m, False) for m in modules)


def _wants_json(request):
    return (
        request.headers.get('x-requested-with') == 'XMLHttpRequest'
        or 'application/json' in request.headers.get('accept', '')
    )


def require(modules=None, level='view', ajax=False):
    """Replaces @login_required + @user_passes_test(...)."""
    def deco(view):
        @wraps(view)
        def wrapped(request, *args, **kwargs):
            json_mode = ajax or _wants_json(request)
            if not request.user.is_authenticated:
                if json_mode:
                    return JsonResponse({'error': 'Login required.'}, status=401)
                return redirect('login')
            if can(request.user, modules, level):
                return view(request, *args, **kwargs)
            if json_mode:
                return JsonResponse({'error': 'Permission denied.'}, status=403)
            messages.error(request, "You don't have access to that page.")
            return redirect('dashboard')
        return wrapped
    return deco