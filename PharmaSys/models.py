# models.py
from django.db import models
from django.contrib.auth.models import User
from django.utils import timezone
from django.db.models import Sum
from django.db.models.signals import post_save
from django.dispatch import receiver
from django.db.models import Sum, Q


# ─────────────────────────────────────────────────────────────────────────────
# USER PROFILE
# ─────────────────────────────────────────────────────────────────────────────
class UserProfile(models.Model):
    ROLE_CHOICES = [
        ('user',  'User'),
        ('staff', 'Staff'),
        ('admin', 'Administrator'),
    ]
    user        = models.OneToOneField(User, on_delete=models.CASCADE, related_name='profile')
    middle_name = models.CharField(max_length=100, blank=True, default='')
    position    = models.CharField(max_length=150, blank=True, default='', verbose_name='Position/Designation')
    role        = models.CharField(max_length=20, choices=ROLE_CHOICES, default='user')
    avatar      = models.ImageField(upload_to='avatars/', blank=True, null=True)

    def __str__(self):
        return f"{self.user.username} — {self.get_role_display()}"

    def get_full_name_with_middle(self):
        parts = [self.user.first_name, self.middle_name, self.user.last_name]
        return ' '.join(p for p in parts if p).strip() or self.user.username
    



# ─────────────────────────────────────────────────────────────────────────────
# USER PERMISSION  (per-user module access checklist)
# ─────────────────────────────────────────────────────────────────────────────
class UserPermission(models.Model):
    user               = models.OneToOneField(User, on_delete=models.CASCADE, related_name='permissions')
    inventory          = models.BooleanField(default=False)
    storage_locations  = models.BooleanField(default=False)
    stock_management   = models.BooleanField(default=False)
    dispensing         = models.BooleanField(default=False)
    dispensing_card    = models.BooleanField(default=False)
    doctors            = models.BooleanField(default=False)
    suppliers          = models.BooleanField(default=False)
    reports            = models.BooleanField(default=False)
    consumption_report = models.BooleanField(default=False)
    returns            = models.BooleanField(default=False)
    pdea_register      = models.BooleanField(default=False)
    stock_card         = models.BooleanField(default=False)
    supplies_ledger    = models.BooleanField(default=False)

    MODULE_CHOICES = [
        ("inventory",          "Medicine Inventory"),
        ("storage_locations",  "Storage Locations"),
        ("stock_management",   "Stock Management"),
        ("dispensing",         "Medicine Dispensing"),
        ("dispensing_card",    "Dispensing Card"),
        ("doctors",            "Doctors"),
        ("suppliers",          "Suppliers"),
        ("reports",            "Reports & Analytics"),
        ("consumption_report", "Consumption Report"),
        ("returns",            "Returns Management"),
        ("pdea_register",      "PDEA Report"),
        ("stock_card",         "Stock Card"),
        ("supplies_ledger",    "Supplies Ledger"),
    ]

    @classmethod
    def module_keys(cls):
        return [key for key, _label in cls.MODULE_CHOICES]

    def as_dict(self):
        return {key: bool(getattr(self, key)) for key in self.module_keys()}

    def __str__(self):
        return f"Permissions — {self.user.username}"

    class Meta:
        verbose_name = "User Permission"


@receiver(post_save, sender=User)
def create_or_save_user_profile(sender, instance, created, **kwargs):
    if created:
        UserProfile.objects.create(user=instance)
        UserPermission.objects.create(user=instance)
    else:
        UserProfile.objects.get_or_create(user=instance)
        UserPermission.objects.get_or_create(user=instance)


# ─────────────────────────────────────────────────────────────────────────────
# SUPPLIER
# ─────────────────────────────────────────────────────────────────────────────
class Supplier(models.Model):
    name         = models.CharField(max_length=200)
    contact_name = models.CharField(max_length=150, blank=True)
    phone        = models.CharField(max_length=50, blank=True)
    email        = models.EmailField(blank=True)
    address      = models.TextField(blank=True)
    notes        = models.TextField(blank=True)

    # ── Regulatory documents ──────────────────────────────────
    pdea_permit_no     = models.CharField(max_length=100, blank=True,
                             verbose_name='PDEA Permit No.')
    pdea_permit_expiry = models.DateField(null=True, blank=True,
                             verbose_name='PDEA Permit Expiry')
    s4_license_no      = models.CharField(max_length=100, blank=True,
                             verbose_name='S4 License No.')
    s4_license_expiry  = models.DateField(null=True, blank=True,
                             verbose_name='S4 License Expiry')

    is_active    = models.BooleanField(default=True)
    created_at   = models.DateTimeField(auto_now_add=True)
    updated_at   = models.DateTimeField(auto_now=True)

    def __str__(self):
        return self.name

    @staticmethod
    def _doc_status(number, expiry):
        """'none' | 'no_expiry' | 'expired' | 'expiring_soon' | 'valid'"""
        if not number:
            return 'none'
        if not expiry:
            return 'no_expiry'
        today = timezone.now().date()
        if expiry < today:
            return 'expired'
        if expiry <= today + timezone.timedelta(days=30):
            return 'expiring_soon'
        return 'valid'

    @property
    def pdea_status(self):
        return self._doc_status(self.pdea_permit_no, self.pdea_permit_expiry)

    @property
    def s4_status(self):
        return self._doc_status(self.s4_license_no, self.s4_license_expiry)

    class Meta:
        ordering = ['name']



class Ward(models.Model):
    code      = models.CharField(max_length=20, unique=True)
    name      = models.CharField(max_length=100)
    is_active = models.BooleanField(default=True)

    def __str__(self):
        return self.name

    @property
    def requisition_label(self):
        """Slash-free label used inside requisition numbers, e.g. '1B-MEDICAL-ISOLATION-CD'."""
        import re
        return re.sub(r'[^A-Za-z0-9]+', '-', self.name).strip('-').upper()

    class Meta:
        ordering = ['code']

# ─────────────────────────────────────────────────────────────────────────────
# DOCTOR  (prescribing physician — required for S2 regulated medicines)
# ─────────────────────────────────────────────────────────────────────────────
class Doctor(models.Model):
    first_name       = models.CharField(max_length=100)
    middle_name      = models.CharField(max_length=100, blank=True)
    last_name        = models.CharField(max_length=100)
    specialization   = models.CharField(max_length=150, blank=True,
                           help_text="Specialization or Department")
    prc_license_no   = models.CharField(max_length=100, unique=True,
                           verbose_name='PRC License No.')
    prc_expiry_date  = models.DateField(null=True, blank=True,
                           verbose_name='PRC License Expiry')
    s2_license_no    = models.CharField(max_length=100, blank=True,
                           verbose_name='S2 License No.',
                           help_text='Leave blank if doctor has no S2 license')
    s2_expiry_date   = models.DateField(null=True, blank=True,
                           verbose_name='S2 License Expiry')
    contact_number   = models.CharField(max_length=50, blank=True)
    clinic_address   = models.TextField(blank=True,
                           verbose_name='Clinic / Hospital Address')
    is_active        = models.BooleanField(default=True)
    created_at       = models.DateTimeField(auto_now_add=True)
    updated_at       = models.DateTimeField(auto_now=True)

    def __str__(self):
        return f"Dr. {self.first_name} {self.last_name}"

    def get_full_name(self):
        parts = [self.first_name, self.middle_name, self.last_name]
        return ' '.join(p for p in parts if p).strip()

    # ── S2 license validity helpers ───────────────────────────
    @property
    def has_s2_license(self):
        """True if a S2 license number is recorded."""
        return bool(self.s2_license_no)

    @property
    def s2_is_expired(self):
        if not self.s2_expiry_date:
            return False
        return self.s2_expiry_date < timezone.now().date()

    @property
    def s2_is_expiring_soon(self):
        """Within 30 days."""
        if not self.s2_expiry_date:
            return False
        threshold = timezone.now().date() + timezone.timedelta(days=30)
        return timezone.now().date() <= self.s2_expiry_date <= threshold

    @property
    def s2_license_status(self):
        """Returns 'none' | 'expired' | 'expiring_soon' | 'valid'"""
        if not self.has_s2_license:
            return 'none'
        if self.s2_is_expired:
            return 'expired'
        if self.s2_is_expiring_soon:
            return 'expiring_soon'
        return 'valid'

    @property
    def s2_is_valid_for_dispensing(self):
        """Hard gate used by the dispensing view — must have license AND not expired."""
        return self.has_s2_license and not self.s2_is_expired

    # ── PRC license validity helpers ──────────────────────────
    @property
    def prc_is_expired(self):
        if not self.prc_expiry_date:
            return False
        return self.prc_expiry_date < timezone.now().date()

    @property
    def prc_is_expiring_soon(self):
        if not self.prc_expiry_date:
            return False
        threshold = timezone.now().date() + timezone.timedelta(days=30)
        return timezone.now().date() <= self.prc_expiry_date <= threshold

    class Meta:
        ordering = ['last_name', 'first_name']
        verbose_name        = 'Doctor'
        verbose_name_plural = 'Doctors'

        

# ─────────────────────────────────────────────────────────────────────────────
# MEDICINE CATEGORY
# ─────────────────────────────────────────────────────────────────────────────
class MedicineCategory(models.Model):
    name        = models.CharField(max_length=100, unique=True)
    description = models.TextField(blank=True)

    def __str__(self):
        return self.name

    class Meta:
        verbose_name_plural = 'Medicine Categories'
        ordering = ['name']



# ─────────────────────────────────────────────────────────────────────────────
# STORAGE AREA  (a room / floor-plan page, e.g. "Main Pharmacy")
# ─────────────────────────────────────────────────────────────────────────────
class StorageArea(models.Model):
    name        = models.CharField(max_length=80, unique=True)
    description = models.CharField(max_length=200, blank=True)
    canvas_w    = models.PositiveIntegerField(default=860)   # aspect ratio only
    canvas_h    = models.PositiveIntegerField(default=540)
    sort_order  = models.PositiveIntegerField(default=0)

    def __str__(self):
        return self.name

    class Meta:
        ordering = ['sort_order', 'name']


# ─────────────────────────────────────────────────────────────────────────────
# STORAGE LOCATION  (a shelf / cabinet / ref drawn on an area's floor plan)
# x, y, w, h are percentages (0-100) of the area canvas.
# ─────────────────────────────────────────────────────────────────────────────
class StorageLocation(models.Model):
    KIND_SHELF    = 'shelf'
    KIND_LANDMARK = 'landmark'
    KIND_CHOICES  = [
        (KIND_SHELF,    'Storage (can hold medicines)'),
        (KIND_LANDMARK, 'Landmark (door, railings, office…)'),
    ]

    area       = models.ForeignKey(StorageArea, on_delete=models.CASCADE, related_name='locations')
    code       = models.CharField(max_length=30, unique=True, help_text='Short unique label, e.g. SR1-G2')
    name       = models.CharField(max_length=80, blank=True)
    kind       = models.CharField(max_length=10, choices=KIND_CHOICES, default=KIND_SHELF)
    x          = models.FloatField(default=5)
    y          = models.FloatField(default=5)
    w          = models.FloatField(default=16)
    h          = models.FloatField(default=12)
    notes      = models.TextField(blank=True)

    LOCK_REASON_CHOICES = [
        ('count',       'Physical Count / Inventory Audit'),
        ('quarantine',  'Quarantine / Under Investigation'),
        ('recall',      'Product Recall'),
        ('temperature', 'Temperature Excursion'),
        ('maintenance', 'Maintenance / Repair / Pest Control'),
        ('inspection',  'Inspection (FDA / PDEA / Internal)'),
        ('other',       'Other'),
    ]
    is_locked   = models.BooleanField(default=False, db_index=True)
    lock_reason = models.CharField(max_length=20, choices=LOCK_REASON_CHOICES, blank=True)
    lock_notes  = models.TextField(blank=True)
    locked_at   = models.DateTimeField(null=True, blank=True)
    locked_by   = models.ForeignKey(User, on_delete=models.SET_NULL, null=True, blank=True,
                                    related_name='locked_locations')
    is_supply_office = models.BooleanField(
        default=False, db_index=True,
        help_text='Supply Office storage: receives stock but is never counted in pharmacy stock')

    created_at = models.DateTimeField(auto_now_add=True)

    def __str__(self):
        return f"{self.code} — {self.name}" if self.name and self.name != self.code else self.code

    class Meta:
        ordering = ['area__sort_order', 'code']





class StockCode(models.Model):
    code         = models.CharField(max_length=20, unique=True)
    generic_name = models.CharField(max_length=200, unique=True)
    created_by   = models.ForeignKey(User, on_delete=models.SET_NULL, null=True, blank=True,
                                     related_name='stock_codes_created')
    created_at   = models.DateTimeField(auto_now_add=True)
    updated_at   = models.DateTimeField(auto_now=True)

    def save(self, *args, **kwargs):
        self.code = (self.code or '').strip().upper()
        self.generic_name = (self.generic_name or '').strip()
        super().save(*args, **kwargs)

    def __str__(self):
        return f'{self.code} — {self.generic_name}'

    def next_sub_code(self):
        prefix = f'{self.code}-'
        nums = []
        for s in Medicine.objects.filter(sub_code__startswith=prefix).values_list('sub_code', flat=True):
            tail = s[len(prefix):]
            if tail.isdigit():
                nums.append(int(tail))
        return f'{prefix}{(max(nums) if nums else 0) + 1:03d}'

    class Meta:
        ordering = ['code']
        verbose_name = 'Stock Code'
        verbose_name_plural = 'Stock Codes'






# ─────────────────────────────────────────────────────────────────────────────
# MEDICINE  (product-level — one row per drug product)
# batch_number / expiry_date / purchase_price moved to MedicineStock
# ─────────────────────────────────────────────────────────────────────────────
class Medicine(models.Model):

    DOSAGE_FORM_CHOICES = [
        ('tablet',              'Tablet'),
        ('effervescent_tablet', 'Effervescent Tablet'),
        ('capsule',             'Capsule'),
        ('syrup',               'Syrup'),
        ('suspension',          'Suspension'),
        ('vial',                'Vial'),
        ('ampule',              'Ampule'),
        ('prefilled_syringe',   'Pre-filled Syringe'),
        ('nebule',              'Nebule'),
        ('sachet',              'Sachet'),
        ('cream',               'Cream'),
        ('ointment',            'Ointment'),
        ('drops',               'Drops'),
        ('inhaler',             'Inhaler'),
        ('patch',               'Patch'),
        ('suppository',         'Suppository'),
    ]

    medicine_name    = models.CharField(max_length=200)
    generic_name     = models.CharField(max_length=200)
    stock_code = models.ForeignKey(
                     'StockCode', on_delete=models.PROTECT,
                     null=True, blank=True, related_name='medicines')
    sub_code   = models.CharField(max_length=40, unique=True, null=True, blank=True,
                     verbose_name='Sub Stock Code')
    brand_name       = models.CharField(max_length=200, blank=True)
    category         = models.ForeignKey(
                           MedicineCategory, on_delete=models.SET_NULL,
                           null=True, blank=True, related_name='medicines')
    dosage_form      = models.CharField(max_length=20, choices=DOSAGE_FORM_CHOICES, default='tablet')
    strength         = models.CharField(max_length=100)
    manufacturer     = models.CharField(max_length=200, blank=True)
    supplier         = models.ForeignKey(
                           Supplier, on_delete=models.SET_NULL,
                           null=True, blank=True, related_name='medicines')
    barcode          = models.CharField(max_length=100, blank=True, unique=True, null=True)
    selling_price    = models.DecimalField(max_digits=10, decimal_places=2, default=0.00)
    reorder_level    = models.PositiveIntegerField(default=10)
    storage_location = models.CharField(max_length=150, blank=True)
    location         = models.ForeignKey(
                           'StorageLocation', on_delete=models.SET_NULL,
                           null=True, blank=True, related_name='medicines')
    is_s2_regulated  = models.BooleanField(default=False,
                           verbose_name='S2 Regulated',
                           help_text='Requires a valid S2 license from the prescribing doctor')
    is_active        = models.BooleanField(default=True)
    created_by       = models.ForeignKey(
                           User, on_delete=models.SET_NULL,
                           null=True, blank=True, related_name='medicines_created')
    created_at       = models.DateTimeField(auto_now_add=True)
    updated_at       = models.DateTimeField(auto_now=True)

    def __str__(self):
        return f"{self.medicine_name} {self.strength}"

    # ── Aggregated stock across all batches ───────────────────
    @property
    def stock_quantity(self):
        result = self.batches.releasable().aggregate(total=Sum('quantity'))
        return result['total'] or 0

    @property
    def locked_quantity(self):
        """Units held in locked locations (excluded from stock_quantity)."""
        result = (self.batches.in_locked_location()
                  .filter(do_not_release=False, quantity__gt=0)
                  .aggregate(total=Sum('quantity')))
        return result['total'] or 0

    @property
    def supply_office_quantity(self):
        """Units held in Supply Office locations (not part of pharmacy stock)."""
        result = (self.batches.in_supply_office()
                  .filter(do_not_release=False, quantity__gt=0)
                  .aggregate(total=Sum('quantity')))
        return result['total'] or 0

    @property
    def is_out_of_stock(self):
        return self.stock_quantity == 0

    @property
    def is_low_stock(self):
        return 0 < self.stock_quantity <= self.reorder_level

    @property
    def is_expiring_soon(self):
        threshold = timezone.now().date() + timezone.timedelta(days=90)
        return self.batches.releasable().filter(
            expiry_date__isnull=False,
            expiry_date__lte=threshold,
            expiry_date__gte=timezone.now().date(),
        ).exists()

    @property
    def is_expired(self):
        return self.batches.releasable().filter(
            expiry_date__isnull=False,
            expiry_date__lt=timezone.now().date(),
        ).exists()

    @property
    def earliest_expiry(self):
        batch = self.batches.releasable().filter(
            expiry_date__isnull=False).order_by('expiry_date').first()
        return batch.expiry_date if batch else None

    @property
    def stock_status(self):
        if self.is_out_of_stock:
            return 'out_of_stock'
        if self.is_low_stock:
            return 'low_stock'
        return 'in_stock'

    # Legacy shim so old code that reads medicine.purchase_price still works
    @property
    def purchase_price(self):
        batch = self.batches.releasable().order_by('expiry_date').first()
        return batch.purchase_price if batch else 0

    class Meta:
        ordering = ['medicine_name']
        verbose_name_plural = 'Medicines'


class MedicineStockQuerySet(models.QuerySet):
    @staticmethod
    def _locked_q():
        return (Q(location__is_locked=True) |
                Q(location__isnull=True, medicine__location__is_locked=True))

    @staticmethod
    def _supply_q():
        return (Q(location__is_supply_office=True) |
                Q(location__isnull=True, medicine__location__is_supply_office=True))

    def in_locked_location(self):
        """Pharmacy stock frozen by a lock (Supply Office stock is reported separately)."""
        return self.filter(self._locked_q()).exclude(self._supply_q())

    def in_supply_office(self):
        return self.filter(self._supply_q())

    def releasable(self):
        """Batches that may be dispensed / counted as pharmacy stock."""
        return (self.filter(do_not_release=False, quantity__gt=0)
                    .exclude(self._locked_q())
                    .exclude(self._supply_q()))


# ─────────────────────────────────────────────────────────────────────────────
# MEDICINE STOCK  (batch-level — one row per delivery / batch)
# ─────────────────────────────────────────────────────────────────────────────
class MedicineStock(models.Model):
    """
    Each row represents one physical batch of a medicine.
    When stock is received (Stock In), a new MedicineStock row is created
    (or an existing matching batch is updated).
    Dispensing and Stock Out deduct from batches in FEFO order
    (earliest expiry first).
    """
    medicine       = models.ForeignKey(
                         Medicine, on_delete=models.CASCADE, related_name='batches')
    batch_number   = models.CharField(max_length=100, blank=True)
    expiry_date    = models.DateField(null=True, blank=True)
    quantity       = models.PositiveIntegerField(default=0)
    purchase_price = models.DecimalField(max_digits=10, decimal_places=2, default=0.00)
    supplier       = models.ForeignKey(
                         Supplier, on_delete=models.SET_NULL,
                         null=True, blank=True, related_name='batches')
    received_at    = models.DateTimeField(auto_now_add=True)
    created_by     = models.ForeignKey(
                         User, on_delete=models.SET_NULL,
                         null=True, blank=True)

    location       = models.ForeignKey(
                         'StorageLocation', on_delete=models.SET_NULL,
                         null=True, blank=True, related_name='batches',
                         help_text='Where this batch physically sits. Blank = the medicine\'s main location')


        # ── Do Not Release (hold) ─────────────────────────────────
    DNR_REASON_CHOICES = [
        ('rts_replacement',    'Return to Supplier - Replacement'),
        ('rts_no_replacement', 'Return to Supplier - No Replacement (Above 18 Months Delivery)'),
        ('rts_credit_memo',    'Return to Supplier - Credit Memo'),
        ('rts_recall',         'Return to Supplier - Product Recall'),
        ('donation',           'For Donation'),
    ]
    do_not_release = models.BooleanField(default=False, db_index=True)
    dnr_reason     = models.CharField(max_length=30, choices=DNR_REASON_CHOICES, blank=True)
    dnr_notes      = models.TextField(blank=True)
    dnr_at         = models.DateTimeField(null=True, blank=True)
    dnr_by         = models.ForeignKey(User, on_delete=models.SET_NULL,
                                       null=True, blank=True, related_name='dnr_batches')

    def __str__(self):
        exp = str(self.expiry_date) if self.expiry_date else 'no expiry'
        return (f"{self.medicine.medicine_name} | "
                f"Batch {self.batch_number or '—'} | "
                f"Exp {exp} | Qty {self.quantity}")

    @property
    def is_expired(self):
        if not self.expiry_date:
            return False
        return self.expiry_date < timezone.now().date()

    @property
    def is_expiring_soon(self):
        if not self.expiry_date:
            return False
        threshold = timezone.now().date() + timezone.timedelta(days=90)
        return timezone.now().date() <= self.expiry_date <= threshold

    class Meta:
        ordering = ['expiry_date', 'received_at']   # FEFO default
        verbose_name        = 'Medicine Stock (Batch)'
        verbose_name_plural = 'Medicine Stock (Batches)'

    objects = MedicineStockQuerySet.as_manager()

    @property
    def effective_location(self):
        return self.location or self.medicine.location

    @property
    def is_locked(self):
        loc = self.effective_location
        return bool(loc and loc.is_locked)

    @property
    def is_supply_office(self):
        loc = self.effective_location
        return bool(loc and loc.is_supply_office)


# ─────────────────────────────────────────────────────────────────────────────
# STOCK MOVEMENT  (audit log — unchanged)
# ─────────────────────────────────────────────────────────────────────────────
class StockMovement(models.Model):

    MOVEMENT_TYPES = [
        ('in',       'Stock In'),
        ('out',      'Stock Out'),
        ('adjust',   'Adjustment'),
        ('return',   'Return'),
        ('expired',  'Expired Removal'),
        ('damaged',  'Damaged / Disposal'),
        ('audit',    'Audit Adjustment'),
        ('transfer', 'Location Transfer'),
        ('so_in',    'Supply Office Receipt'),
    ]

    medicine        = models.ForeignKey(
                          Medicine, on_delete=models.CASCADE, related_name='movements')
    # Optional link to the specific batch this movement touched
    batch           = models.ForeignKey(
                          MedicineStock, on_delete=models.SET_NULL,
                          null=True, blank=True, related_name='movements')
    movement_type   = models.CharField(max_length=20, choices=MOVEMENT_TYPES)
    quantity        = models.IntegerField()
    quantity_before = models.PositiveIntegerField()   # total across all batches before
    quantity_after  = models.PositiveIntegerField()   # total across all batches after
    supplier        = models.ForeignKey(
                          Supplier, on_delete=models.SET_NULL,
                          null=True, blank=True)
    batch_number    = models.CharField(max_length=100, blank=True)
    expiry_date     = models.DateField(null=True, blank=True)
    purchase_price  = models.DecimalField(max_digits=10, decimal_places=2, null=True, blank=True)
    dispensed_to    = models.CharField(max_length=200, blank=True)
    prescription_no = models.CharField(max_length=100, blank=True)
    notes           = models.TextField(blank=True)
    reference_no    = models.CharField(max_length=100, blank=True)
    performed_by    = models.ForeignKey(
                          User, on_delete=models.SET_NULL,
                          null=True, blank=True, related_name='stock_movements')
    created_at      = models.DateTimeField(auto_now_add=True)

    def __str__(self):
        return (f"{self.get_movement_type_display()} — "
                f"{self.medicine.medicine_name} ({self.quantity:+d})")

    @property
    def is_stock_in(self):
        return self.movement_type in ('in', 'return')

    @property
    def is_stock_out(self):
        return self.movement_type in ('out', 'expired', 'damaged')

    class Meta:
        ordering = ['-created_at']
        verbose_name        = 'Stock Movement'
        verbose_name_plural = 'Stock Movements'


# ─────────────────────────────────────────────────────────────────────────────
# DISPENSING  (transaction header)
# ─────────────────────────────────────────────────────────────────────────────
class Dispensing(models.Model):
    customer_name   = models.CharField(max_length=200, blank=True)
    prescription_no = models.CharField(max_length=100, blank=True)
    notes           = models.TextField(blank=True)
    subtotal_amount = models.DecimalField(max_digits=12, decimal_places=2, default=0)
    discount_amount = models.DecimalField(max_digits=12, decimal_places=2, default=0)
    total_amount    = models.DecimalField(max_digits=12, decimal_places=2, default=0)


    ihomis_hpatkey = models.CharField(
                     max_length=50, blank=True, default='',
                     db_index=True,
                     verbose_name='iHOMIS Patient Key',
                     help_text='hpatkey from iHOMIS hperson table')
    ihomis_hpercode = models.CharField(
                          max_length=50, blank=True, default='',
                          verbose_name='Hospital Number',
                          help_text='Cached hospital number for display')


    PATIENT_TYPE_CHOICES = [('in', 'In Patient'), ('out', 'Out Patient')]
    patient_type     = models.CharField(max_length=3, choices=PATIENT_TYPE_CHOICES,
                                        blank=True, default='')
    ward             = models.ForeignKey('Ward', on_delete=models.PROTECT,
                                         null=True, blank=True, related_name='dispensings')
    requisition_no   = models.CharField(max_length=100, blank=True, default='', db_index=True)
    requisition_seq  = models.PositiveIntegerField(null=True, blank=True)  # In Patient running no.
    requisition_year = models.PositiveIntegerField(null=True, blank=True)
    requisition_half = models.PositiveSmallIntegerField(null=True, blank=True)  # 1 = Jan–Jun, 2 = Jul–Dec



    doctor          = models.ForeignKey(
                          'Doctor', on_delete=models.SET_NULL,
                          null=True, blank=True, related_name='dispensings',
                          help_text='Required when dispensing S2 regulated medicines')
    pharmacist      = models.ForeignKey(
                          User, on_delete=models.SET_NULL,
                          null=True, blank=True, related_name='dispensings')
    created_at      = models.DateTimeField(auto_now_add=True)

    def __str__(self):
        return f"Dispensing #{self.pk} — ₱{self.total_amount} ({self.created_at.date()})"

    class Meta:
        ordering = ['-created_at']
        verbose_name        = 'Dispensing Transaction'
        verbose_name_plural = 'Dispensing Transactions'


# ─────────────────────────────────────────────────────────────────────────────
# DISPENSING ITEM  (line item per medicine in a transaction)
# ─────────────────────────────────────────────────────────────────────────────
class DispensingItem(models.Model):
    dispensing  = models.ForeignKey(
                      Dispensing, on_delete=models.CASCADE, related_name='items')
    medicine    = models.ForeignKey(
                      Medicine, on_delete=models.PROTECT, related_name='dispensing_items')
    quantity    = models.PositiveIntegerField()
    unit_price  = models.DecimalField(max_digits=10, decimal_places=2)
    subtotal    = models.DecimalField(max_digits=12, decimal_places=2)

    def save(self, *args, **kwargs):
        self.subtotal = self.unit_price * self.quantity
        super().save(*args, **kwargs)

    def __str__(self):
        return f"{self.medicine.medicine_name} x{self.quantity} @ ₱{self.unit_price}"

    class Meta:
        verbose_name        = 'Dispensing Item'
        verbose_name_plural = 'Dispensing Items'



 
class MedicineReturn(models.Model):
    """
    Header row for one return event.
      source = 'patient' → built from a patient's dispensing history (Tab 1)
      source = 'log'     → free-form Return Log (Tab 2)
    """
    SOURCE_PATIENT = 'patient'
    SOURCE_LOG     = 'log'
    SOURCE_CHOICES = [
        (SOURCE_PATIENT, 'Patient Return'),
        (SOURCE_LOG,     'Return Log'),
    ]
 
    REASON_CHOICES = [
        ('discharged', 'Patient Discharged'),
        ('expired',    'Patient Expired'),
        ('ward',       'Returned from Ward'),
        ('others',     'Others'),
    ]
 
    source          = models.CharField(max_length=10, choices=SOURCE_CHOICES, default=SOURCE_LOG)
    patient_name    = models.CharField(max_length=200, blank=True)
    ihomis_hpatkey  = models.CharField(max_length=50, blank=True, default='', db_index=True)
    ihomis_hpercode = models.CharField(max_length=50, blank=True, default='')
    reason          = models.CharField(max_length=20, choices=REASON_CHOICES)
    notes           = models.TextField(blank=True)
    restocked       = models.BooleanField(
                          default=True,
                          help_text='True when the returned quantities were added back to stock')
    returned_at     = models.DateTimeField(default=timezone.now)
    processed_by    = models.ForeignKey(
                          User, on_delete=models.SET_NULL,
                          null=True, blank=True, related_name='medicine_returns')
    created_at      = models.DateTimeField(auto_now_add=True)
 
    def __str__(self):
        return f"Return #{self.pk} — {self.patient_name or 'N/A'} ({self.get_reason_display()})"
 
    class Meta:
        ordering = ['-returned_at']
        verbose_name        = 'Medicine Return'
        verbose_name_plural = 'Medicine Returns'
 
 
class MedicineReturnItem(models.Model):
    medicine_return = models.ForeignKey(
                          MedicineReturn, on_delete=models.CASCADE, related_name='items')
    medicine        = models.ForeignKey(
                          Medicine, on_delete=models.PROTECT, related_name='return_items')
    # Set for Tab 1 returns so we can track how much of a dispensed line is already returned
    dispensing_item = models.ForeignKey(
                          DispensingItem, on_delete=models.SET_NULL,
                          null=True, blank=True, related_name='returns')
    quantity        = models.PositiveIntegerField()
 
    # The batch these units went back into (blank for record-only returns).
    # batch_number / expiry_date are snapshots so reports stay correct even if
    # the batch is edited later.
    batch           = models.ForeignKey(
                          MedicineStock, on_delete=models.SET_NULL,
                          null=True, blank=True, related_name='return_items')
    batch_number    = models.CharField(max_length=100, blank=True)
    expiry_date     = models.DateField(null=True, blank=True)
 
    def __str__(self):
        return f"{self.medicine.medicine_name} x{self.quantity}"
 
    class Meta:
        verbose_name        = 'Medicine Return Item'
        verbose_name_plural = 'Medicine Return Items'