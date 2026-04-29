# ihomis_models.py
# Read-only models mapping to the iHOMIS secondary database (hospital_test).
# These are NEVER migrated — they reflect existing iHOMIS tables as-is.

from django.db import models


class HPerson(models.Model):
    """
    Maps to [hospital_test].[dbo].[hperson]
    Read-only — managed by iHOMIS, not by this application.
    """
    
    hpatkey = models.CharField(max_length=50, primary_key=True) # iHOMIS PK
    hpercode    = models.CharField(max_length=50, blank=True)  # Hospital Number
    hpatcode    = models.CharField(max_length=50, blank=True)  # Alt patient code
    patmedno    = models.CharField(max_length=50, blank=True)  # Medical record no.

    # Name
    patlast     = models.CharField(max_length=100, blank=True)
    patfirst    = models.CharField(max_length=100, blank=True)
    patmiddle   = models.CharField(max_length=100, blank=True)
    patsuffix   = models.CharField(max_length=20,  blank=True)

    # Demographics
    patbdate    = models.DateField(null=True, blank=True)
    patsex      = models.CharField(max_length=10,  blank=True)
    patcstat    = models.CharField(max_length=20,  blank=True)  # Civil status

    # Address components
    pat_unit    = models.CharField(max_length=100, blank=True)
    pat_bldg    = models.CharField(max_length=100, blank=True)
    pat_lot     = models.CharField(max_length=100, blank=True)
    pat_subd    = models.CharField(max_length=150, blank=True)
    citcode     = models.CharField(max_length=20,  blank=True)  # City/municipality code

    # Contact
    pattelno    = models.CharField(max_length=50,  blank=True)
    pat_email   = models.CharField(max_length=150, blank=True)

    # PhilHealth
    phicnum     = models.CharField(max_length=50,  blank=True)

    created_at  = models.DateTimeField(null=True, blank=True)

    # ── Computed helpers ──────────────────────────────────────
    @property
    def full_name(self):
        parts = [self.patfirst, self.patmiddle, self.patlast]
        name  = ' '.join(p for p in parts if p).strip()
        if self.patsuffix:
            name += f' {self.patsuffix}'
        return name or '—'

    @property
    def full_name_formal(self):
        """LAST, First Middle Suffix — for formal documents."""
        last  = self.patlast.upper() if self.patlast else ''
        first = self.patfirst or ''
        mid   = self.patmiddle or ''
        suf   = self.patsuffix or ''
        parts = [last + ',', first, mid]
        name  = ' '.join(p for p in parts if p.strip()).strip().rstrip(',')
        if suf:
            name += f' {suf}'
        return name or '—'

    @property
    def address(self):
        parts = [self.pat_unit, self.pat_bldg, self.pat_lot, self.pat_subd]
        return ', '.join(p for p in parts if p).strip() or '—'

    @property
    def hospital_number(self):
        return self.hpercode or self.hpatcode or str(self.hpatkey)

    @property
    def sex_display(self):
        s = (self.patsex or '').strip().upper()
        if s in ('M', 'MALE'):
            return 'Male'
        if s in ('F', 'FEMALE'):
            return 'Female'
        return self.patsex or '—'

    def __str__(self):
        return f"{self.hospital_number} — {self.full_name}"

    class Meta:
        managed     = False           # Never run migrations on this table
        db_table    = 'hperson'       # Exact table name in iHOMIS DB
        ordering    = ['patlast', 'patfirst']
        verbose_name        = 'iHOMIS Patient'
        verbose_name_plural = 'iHOMIS Patients'