from django.db import migrations
from django.db.models import Q

APP = 'PharmaSys'
FIELDS = ['inventory', 'stock_management', 'dispensing', 'dispensing_card',
          'doctors', 'suppliers', 'reports', 'consumption_report']


def forwards(apps, schema_editor):
    User = apps.get_model('auth', 'User')
    UserProfile = apps.get_model(APP, 'UserProfile')
    UserPermission = apps.get_model(APP, 'UserPermission')

    for u in User.objects.filter(profile__isnull=True):
        UserProfile.objects.create(user=u)
    for u in User.objects.filter(permissions__isnull=True):
        UserPermission.objects.create(user=u)

    # keep storage access for everyone who had Inventory
    UserPermission.objects.filter(inventory=True).update(storage_locations=True)

    # "user" accounts with any module become "staff" so they keep add/edit rights
    q = Q()
    for f in FIELDS:
        q |= Q(**{f: True})
    ids = list(UserPermission.objects.filter(q).values_list('user_id', flat=True))
    UserProfile.objects.filter(role='user', user_id__in=ids).update(role='staff')


class Migration(migrations.Migration):
    dependencies = [
        ('PharmaSys', '0017_userpermission_storage_locations'),
    ]
    operations = [migrations.RunPython(forwards, migrations.RunPython.noop)]