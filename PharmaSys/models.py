# models.py
from django.db import models
from django.contrib.auth.models import User
from django.utils import timezone
from django.db.models import Sum
from django.db.models.signals import post_save
from django.dispatch import receiver


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
    stock_management   = models.BooleanField(default=False)
    dispensing         = models.BooleanField(default=False)
    dispensing_card    = models.BooleanField(default=False)
    doctors            = models.BooleanField(default=False)
    suppliers          = models.BooleanField(default=False)
    reports            = models.BooleanField(default=False)
    consumption_report = models.BooleanField(default=False)

    MODULE_CHOICES = [
        ("inventory",          "Medicine Inventory"),
        ("stock_management",   "Stock Management"),
        ("dispensing",         "Medicine Dispensing"),
        ("dispensing_card",    "Dispensing Card"),
        ("doctors",            "Doctors"),
        ("suppliers",          "Suppliers"),
        ("reports",            "Reports & Analytics"),
        ("consumption_report", "Consumption Report"),
    ]

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
    is_active    = models.BooleanField(default=True)
    created_at   = models.DateTimeField(auto_now_add=True)
    updated_at   = models.DateTimeField(auto_now=True)

    def __str__(self):
        return self.name

    class Meta:
        ordering = ['name']


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
# MEDICINE  (product-level — one row per drug product)
# batch_number / expiry_date / purchase_price moved to MedicineStock
# ─────────────────────────────────────────────────────────────────────────────
class Medicine(models.Model):

    DOSAGE_FORM_CHOICES = [
        ('tablet',      'Tablet'),
        ('capsule',     'Capsule'),
        ('syrup',       'Syrup'),
        ('suspension',  'Suspension'),
        ('injection',   'Injection'),
        ('cream',       'Cream'),
        ('ointment',    'Ointment'),
        ('drops',       'Drops'),
        ('inhaler',     'Inhaler'),
        ('patch',       'Patch'),
        ('suppository', 'Suppository'),
        ('powder',      'Powder'),
        ('other',       'Other'),
    ]

    medicine_name    = models.CharField(max_length=200)
    generic_name     = models.CharField(max_length=200)
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
        result = self.batches.filter(quantity__gt=0).aggregate(total=Sum('quantity'))
        return result['total'] or 0

    @property
    def is_out_of_stock(self):
        return self.stock_quantity == 0

    @property
    def is_low_stock(self):
        return 0 < self.stock_quantity <= self.reorder_level

    @property
    def is_expiring_soon(self):
        """True if the earliest-expiring active batch expires within 90 days."""
        threshold = timezone.now().date() + timezone.timedelta(days=90)
        return self.batches.filter(
            quantity__gt=0,
            expiry_date__isnull=False,
            expiry_date__lte=threshold,
            expiry_date__gte=timezone.now().date(),
        ).exists()

    @property
    def is_expired(self):
        """True if any active batch is expired."""
        return self.batches.filter(
            quantity__gt=0,
            expiry_date__isnull=False,
            expiry_date__lt=timezone.now().date(),
        ).exists()

    @property
    def earliest_expiry(self):
        """The soonest expiry date among active batches, or None."""
        batch = self.batches.filter(
            quantity__gt=0, expiry_date__isnull=False
        ).order_by('expiry_date').first()
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
        batch = self.batches.filter(quantity__gt=0).order_by('expiry_date').first()
        return batch.purchase_price if batch else 0

    class Meta:
        ordering = ['medicine_name']
        verbose_name_plural = 'Medicines'


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