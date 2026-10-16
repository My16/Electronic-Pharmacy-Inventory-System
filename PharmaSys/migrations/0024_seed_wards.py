from django.db import migrations

WARDS = [
    ('10011', 'ICU'), ('10012', 'NICU'), ('10017', 'PICU'),
    ('20001', '1A Medical'), ('20002', '1B Medical Isolation/ CD'),
    ('20003', '1C IM'), ('20005', '2B IM'),
    ('20006', '3A Ortho Pedia'), ('20007', '3B Ortho Adult'),
    ('20008', '4 Private'), ('20009', '5A OB'), ('20010', '5B OB Post Op'),
    ('20011', '6 Surgery'), ('20014', 'C4 Pedia'), ('20017', 'Acute Stroke Unit'),
    ('20019', '5C Gyne'), ('20020', 'C2 Private(Wing B)'),
    ('20021', 'C2 Infectious(Wing A)'), ('30001', '2B Neuro'),
    ('30002', '1C FAMED'), ('30003', '2A Private FAMED'), ('30004', '2A Private IM'),
]

def seed_wards(apps, schema_editor):
    Ward = apps.get_model('YOUR_APP_NAME', 'Ward')
    for code, name in WARDS:
        Ward.objects.update_or_create(code=code, defaults={'name': name})
        
class Migration(migrations.Migration):

    dependencies = [
        ('PharmaSys', '0023_ward_dispensing_patient_type_and_more'),
    ]

    operations = [
    ]
