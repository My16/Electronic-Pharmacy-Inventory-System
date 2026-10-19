# views.py
from django.shortcuts import render, redirect, get_object_or_404
from django.contrib.auth import authenticate, login, logout
from django.contrib.auth.decorators import login_required, user_passes_test
from django.contrib.auth.models import User
from django.contrib import messages
from django.http import JsonResponse, HttpResponse
from django.utils import timezone
from django.db.models import (
    Q, Sum, Count, F, Value, DecimalField,
    ExpressionWrapper, FloatField, Prefetch
)
from django.db.models.functions import TruncDay, TruncWeek, TruncMonth
from django.core.paginator import Paginator, EmptyPage, PageNotAnInteger
from .models import (
    UserProfile, UserPermission, Medicine, MedicineCategory, MedicineStock,
    Supplier, StockMovement, Dispensing, DispensingItem, Doctor, Ward
)
import json
import io

from django.views.decorators.http   import require_GET

from .models          import Dispensing, DispensingItem, StorageArea, StorageLocation, Medicine, MedicineCategory, MedicineStock, Supplier, StockMovement, Doctor
from .ihomis_models   import HPerson

from datetime import date, datetime, timedelta

try:
    import openpyxl
    from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
    from openpyxl.utils import get_column_letter
    OPENPYXL_AVAILABLE = True
except ImportError:
    OPENPYXL_AVAILABLE = False

try:
    from reportlab.lib.pagesizes import A4, landscape
    from reportlab.lib.units import inch
    from reportlab.lib import colors
    from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
    from reportlab.lib.units import cm
    from reportlab.platypus import (
        SimpleDocTemplate, Table, TableStyle, Paragraph,
        Spacer, HRFlowable
    )
    from reportlab.lib.enums import TA_CENTER, TA_LEFT, TA_RIGHT
    REPORTLAB_AVAILABLE = True
except ImportError:
    REPORTLAB_AVAILABLE = False


from django.db import transaction
import calendar

from django.db.models import Q, Sum, Count, F, Value, DecimalField, ExpressionWrapper, FloatField, Prefetch, Min   # + Min
from django.views.decorators.http import require_GET, require_POST                                                   # + require_POST
from django.urls import reverse
from urllib.parse import urlencode

from django.utils.http import url_has_allowed_host_and_scheme
from .permissions import require, can, get_role, is_admin

from django.db.models import (
    Q, Sum, Count, F, Value, DecimalField,
    ExpressionWrapper, FloatField, Prefetch,
    Case, When, IntegerField
)


import json
 
from django.shortcuts import render, get_object_or_404
from django.http import JsonResponse
from django.db import transaction
from django.db.models import Sum, F
from django.utils import timezone
from django.views.decorators.http import require_GET, require_POST
 
from .models import (
    Medicine, MedicineStock, StockMovement, Dispensing, DispensingItem,
    MedicineReturn, MedicineReturnItem,
)
from .ihomis_models import HPerson
from .permissions import require

from django.db.models import Max
import re
import math
from xml.sax.saxutils import escape as _xml_escape



def _deduct_fefo(medicine, qty_needed, performed_by,
                 movement_type='out', dispensed_to='',
                 prescription_no='', notes='', reference_no=''):
    """
    Deduct `qty_needed` units from `medicine` using FEFO
    (earliest expiry first, then batches with no expiry).
    Creates one StockMovement log entry per batch touched.
    Returns True on success, raises ValueError if insufficient stock.
    """
    total_available = medicine.stock_quantity
    if qty_needed > total_available:
        raise ValueError(
            f'Insufficient stock for "{medicine.medicine_name}". '
            f'Available: {total_available}.'
        )

    # Batches ordered: soonest expiry first, then no-expiry batches, then oldest received
    batches = list(
        medicine.batches.filter(quantity__gt=0)
        .order_by(
            F('expiry_date').asc(nulls_last=True),
            'received_at'
        )
    )

    remaining = qty_needed
    for batch in batches:
        if remaining <= 0:
            break
        take = min(batch.quantity, remaining)
        old_total = medicine.stock_quantity          # before this batch deduction
        batch.quantity -= take
        batch.save()
        new_total = medicine.stock_quantity          # after

        StockMovement.objects.create(
            medicine        = medicine,
            batch           = batch,
            movement_type   = movement_type,
            quantity        = -take,
            quantity_before = old_total,
            quantity_after  = new_total,
            batch_number    = batch.batch_number,
            expiry_date     = batch.expiry_date,
            purchase_price  = batch.purchase_price,
            supplier        = batch.supplier,
            dispensed_to    = dispensed_to,
            prescription_no = prescription_no,
            reference_no    = reference_no,
            notes           = notes,
            performed_by    = performed_by,
        )
        remaining -= take

    return True


def _next_requisition(ward, year, half):
    """Next In Patient requisition for this ward + year + half → (seq, '0001/WARD/2026')."""
    last = (Dispensing.objects
            .filter(patient_type='in', ward=ward,
                    requisition_year=year, requisition_half=half)
            .aggregate(m=Max('requisition_seq'))['m'] or 0)
    seq = last + 1
    return seq, f'{seq:04d}/{ward.requisition_label}/{year}'


def _parse_date_range(request, default_days=30):
    today     = timezone.now().date()
    date_from = request.GET.get('date_from', '')
    date_to   = request.GET.get('date_to', '')
    try:
        date_from = date.fromisoformat(date_from)
    except (ValueError, TypeError):
        date_from = today - timedelta(days=default_days)
    try:
        date_to = date.fromisoformat(date_to)
    except (ValueError, TypeError):
        date_to = today
    return date_from, date_to


# ─────────────────────────────────────────────────────────────────────────────
# AUTH
# ─────────────────────────────────────────────────────────────────────────────

def login_view(request):
    if request.user.is_authenticated:
        return redirect('dashboard')
    if request.method == 'POST':
        username = request.POST.get('username', '').strip()
        password = request.POST.get('password', '')
        if not username or not password:
            messages.error(request, 'Please enter both username and password.')
            return render(request, 'loginpage.html')
        user = authenticate(request, username=username, password=password)
        if user is not None:
            if user.is_active:
                login(request, user)
                nxt = request.GET.get('next', '')
                if nxt and url_has_allowed_host_and_scheme(nxt, allowed_hosts={request.get_host()}):
                    return redirect(nxt)
                return redirect('dashboard')
            messages.error(request, 'Your account has been disabled.')
        else:
            messages.error(request, 'Invalid username or password.')
    return render(request, 'loginpage.html')


def logout_view(request):
    logout(request)
    return redirect('login')


# ─────────────────────────────────────────────────────────────────────────────
# DASHBOARD
# ─────────────────────────────────────────────────────────────────────────────

@login_required(login_url='login')
def dashboard_view(request):
    from dateutil.relativedelta import relativedelta

    today            = timezone.now().date()
    expiry_threshold = today + timezone.timedelta(days=90)

    active_medicines = Medicine.objects.filter(is_active=True)
    total_medicines  = active_medicines.count()

    # Low / out counts use annotated total stock per medicine
    med_with_stock = active_medicines.annotate(
        total_stock=Sum('batches__quantity')
    )
    low_stock_count    = med_with_stock.filter(
                             total_stock__gt=0,
                             total_stock__lte=F('reorder_level')).count()
    out_of_stock_count = med_with_stock.filter(
                             total_stock__isnull=True).count() + \
                         med_with_stock.filter(total_stock=0).count()

    expiring_soon_count = MedicineStock.objects.filter(
                              medicine__is_active=True,
                              quantity__gt=0,
                              expiry_date__lte=expiry_threshold,
                              expiry_date__gte=today).values('medicine').distinct().count()

    # Total stock value: sum(selling_price * batch_quantity) per medicine
    stock_value_qs = MedicineStock.objects.filter(
        medicine__is_active=True, quantity__gt=0
    ).annotate(
        line_value=ExpressionWrapper(
            F('medicine__selling_price') * F('quantity'),
            output_field=DecimalField()
        )
    ).aggregate(total=Sum('line_value'))
    total_stock_value = stock_value_qs['total'] or 0

    today_qs              = StockMovement.objects.filter(created_at__date=today)
    total_in_today        = today_qs.filter(movement_type__in=('in', 'return')).aggregate(
                                t=Sum('quantity'))['t'] or 0
    total_out_today       = abs(today_qs.filter(movement_type__in=('out', 'expired', 'damaged')).aggregate(
                                t=Sum('quantity'))['t'] or 0)
    total_movements_today = today_qs.count()

    recent_movements = StockMovement.objects.select_related(
                           'medicine', 'performed_by').order_by('-created_at')[:8]

    # Low stock items — medicines whose total batch qty <= reorder_level
    low_stock_items = []
    for m in active_medicines.prefetch_related('batches'):
        if 0 < m.stock_quantity <= m.reorder_level:
            low_stock_items.append(m)
        if len(low_stock_items) >= 8:
            break

    expiring_items = MedicineStock.objects.filter(
        medicine__is_active=True,
        quantity__gt=0,
        expiry_date__lte=expiry_threshold,
        expiry_date__gte=today
    ).select_related('medicine').order_by('expiry_date')[:6]

    monthly_labels, monthly_in, monthly_out = [], [], []
    for i in range(5, -1, -1):
        month_start = today.replace(day=1) - relativedelta(months=i)
        month_end   = month_start + relativedelta(months=1)
        qs          = StockMovement.objects.filter(
                          created_at__date__gte=month_start,
                          created_at__date__lt=month_end)
        in_qty  = qs.filter(movement_type__in=('in', 'return')).aggregate(
                      t=Sum('quantity'))['t'] or 0
        out_qty = abs(qs.filter(movement_type__in=('out', 'expired', 'damaged')).aggregate(
                      t=Sum('quantity'))['t'] or 0)
        monthly_labels.append(month_start.strftime('%b %Y'))
        monthly_in.append(in_qty)
        monthly_out.append(out_qty)

    weekly_labels, weekly_in, weekly_out = [], [], []
    for i in range(6, -1, -1):
        day     = today - timezone.timedelta(days=i)
        qs      = StockMovement.objects.filter(created_at__date=day)
        in_qty  = qs.filter(movement_type__in=('in', 'return')).aggregate(
                      t=Sum('quantity'))['t'] or 0
        out_qty = abs(qs.filter(movement_type__in=('out', 'expired', 'damaged')).aggregate(
                      t=Sum('quantity'))['t'] or 0)
        weekly_labels.append(day.strftime('%a %d'))
        weekly_in.append(in_qty)
        weekly_out.append(out_qty)

    context = {
        'total_medicines':       total_medicines,
        'low_stock_count':       low_stock_count,
        'out_of_stock_count':    out_of_stock_count,
        'expiring_soon_count':   expiring_soon_count,
        'total_stock_value':     total_stock_value,
        'total_in_today':        total_in_today,
        'total_out_today':       total_out_today,
        'total_movements_today': total_movements_today,
        'recent_movements':      recent_movements,
        'low_stock_items':       low_stock_items,
        'expiring_items':        expiring_items,
        'monthly_labels': json.dumps(monthly_labels),
        'monthly_in':     json.dumps(monthly_in),
        'monthly_out':    json.dumps(monthly_out),
        'weekly_labels':  json.dumps(weekly_labels),
        'weekly_in':      json.dumps(weekly_in),
        'weekly_out':     json.dumps(weekly_out),
    }
    return render(request, 'dashboard.html', context)


# ─────────────────────────────────────────────────────────────────────────────
# INVENTORY
# ─────────────────────────────────────────────────────────────────────────────


def _attach_locations(meds):
    """Adds m.loc_list = [{'loc': StorageLocation, 'qty': int}], main location first.
    A batch with no location counts as the medicine's main location."""
    splits = {}
    for m in meds:
        s = {}
        for b in m.batches.all():              # already prefetched
            if b.quantity <= 0:
                continue
            lid = b.location_id or m.location_id
            if lid:
                s[lid] = s.get(lid, 0) + b.quantity
        if not s and m.location_id:            # main location assigned but no stock yet
            s[m.location_id] = 0
        splits[m.pk] = s

    ids  = {lid for s in splits.values() for lid in s}
    locs = StorageLocation.objects.select_related('area').in_bulk(ids)
    for m in meds:
        items = [{'loc': locs[lid], 'qty': q} for lid, q in splits[m.pk].items() if lid in locs]
        items.sort(key=lambda i: (i['loc'].pk != m.location_id, -i['qty']))
        m.loc_list = items


@require('inventory')
def inventory_view(request):
    today            = timezone.now().date()
    expiry_threshold = today + timezone.timedelta(days=90)

    search        = request.GET.get('search', '').strip()
    category_id   = request.GET.get('category', '')
    status_filter = request.GET.get('status', '')
    dosage_filter = request.GET.get('dosage', '')
    highlight_pk  = request.GET.get('highlight', '')
    per_page      = request.GET.get('per_page', '15')
    try:
        per_page = int(per_page)
        if per_page not in [10, 15, 25, 50, 100]:
            per_page = 15
    except ValueError:
        per_page = 15

    medicines_qs = Medicine.objects.select_related('category', 'supplier', 'location').filter(
        is_active=True
    ).prefetch_related(
        Prefetch('batches', queryset=MedicineStock.objects.select_related('location', 'supplier'))
    )

    if search:
        medicines_qs = medicines_qs.filter(
            Q(medicine_name__icontains=search) | Q(generic_name__icontains=search) |
            Q(brand_name__icontains=search)    | Q(barcode__icontains=search)       |
            Q(batches__batch_number__icontains=search)
        ).distinct()
    if category_id:
        medicines_qs = medicines_qs.filter(category_id=category_id)
    if dosage_filter:
        medicines_qs = medicines_qs.filter(dosage_form=dosage_filter)

    # Status filters require Python-level evaluation because stock_quantity is a property
    if status_filter in ('low_stock', 'out_of_stock', 'expiring', 'expired'):
        all_meds = list(medicines_qs)
        if status_filter == 'low_stock':
            medicines_qs = [m for m in all_meds if m.is_low_stock]
        elif status_filter == 'out_of_stock':
            medicines_qs = [m for m in all_meds if m.is_out_of_stock]
        elif status_filter == 'expiring':
            medicines_qs = [m for m in all_meds if m.is_expiring_soon and not m.is_expired]
        elif status_filter == 'expired':
            medicines_qs = [m for m in all_meds if m.is_expired]

    if highlight_pk:
        try:
            highlighted  = [m for m in medicines_qs if str(m.pk) == str(highlight_pk)]
            rest         = [m for m in medicines_qs if str(m.pk) != str(highlight_pk)]
            medicines_qs = highlighted + rest
        except Exception:
            pass

    paginator   = Paginator(medicines_qs, per_page)
    page_number = request.GET.get('page', 1)
    try:
        page_obj = paginator.page(page_number)
    except (EmptyPage, PageNotAnInteger):
        page_obj = paginator.page(1)

    rows = list(page_obj.object_list)
    _attach_locations(rows)

    # Alert panel items
    all_active = list(Medicine.objects.filter(is_active=True).prefetch_related('batches'))
    low_stock_items   = sorted([m for m in all_active if m.is_low_stock],
                                key=lambda m: m.stock_quantity)[:10]
    out_of_stock_items = [m for m in all_active if m.is_out_of_stock][:10]

    expiring_batches = MedicineStock.objects.filter(
        medicine__is_active=True,
        quantity__gt=0,
        expiry_date__lte=expiry_threshold,
        expiry_date__gte=today,
    ).select_related('medicine').order_by('expiry_date')[:10]

    context = {
        'page_obj':     page_obj,
        'medicines':    rows,
        'paginator':    paginator,
        'per_page':     per_page,
        'per_page_options': [10, 15, 25, 50, 100],
        'search':         search,
        'category_id':    category_id,
        'status_filter':  status_filter,
        'dosage_filter':  dosage_filter,
        'highlight_pk':   highlight_pk,
        'categories':     MedicineCategory.objects.all(),
        'suppliers':      Supplier.objects.filter(is_active=True),
        'storage_areas': get_storage_areas(),
        'dosage_choices': Medicine.DOSAGE_FORM_CHOICES,
        'low_stock_items':    low_stock_items,
        'expiring_batches':   expiring_batches,
        'out_of_stock_items': out_of_stock_items,
        'total_medicines':     len(all_active),
        'low_stock_count':     len([m for m in all_active if m.is_low_stock]),
        'out_of_stock_count':  len([m for m in all_active if m.is_out_of_stock]),
        'expiring_soon_count': MedicineStock.objects.filter(
                                   medicine__is_active=True, quantity__gt=0,
                                   expiry_date__lte=expiry_threshold,
                                   expiry_date__gte=today).values('medicine').distinct().count(),
        'today': today,
        'can_manage': can(request.user, 'inventory', 'edit'),
        'can_admin':  is_admin(request.user),
    }
    return render(request, 'inventory.html', context)


@require('inventory', 'edit')
def inventory_add_view(request):
    """
    Adds a new Medicine product. The initial stock (if any) creates a
    MedicineStock batch and a StockMovement record.
    """
    if request.method == 'POST':
        try:
            barcode = request.POST.get('barcode', '').strip() or None
            if barcode and Medicine.objects.filter(barcode=barcode).exists():
                messages.error(request, f'Barcode "{barcode}" already exists.')
                return redirect('inventory')

            medicine = Medicine.objects.create(
                medicine_name    = request.POST.get('medicine_name', '').strip(),
                generic_name     = request.POST.get('generic_name', '').strip(),
                brand_name       = request.POST.get('brand_name', '').strip(),
                category_id      = request.POST.get('category') or None,
                dosage_form      = request.POST.get('dosage_form', 'tablet'),
                strength         = request.POST.get('strength', '').strip(),
                manufacturer     = request.POST.get('manufacturer', '').strip(),
                supplier_id      = request.POST.get('supplier') or None,
                barcode          = barcode,
                selling_price    = request.POST.get('selling_price') or 0,
                reorder_level    = int(request.POST.get('reorder_level') or 10),
                location_id      = request.POST.get('location') or None,
                is_s2_regulated  = request.POST.get('is_s2_regulated') == '1',
                created_by       = request.user,
            )

            # If initial stock provided, create the first batch
            init_qty = int(request.POST.get('stock_quantity') or 0)
            if init_qty > 0:
                batch = MedicineStock.objects.create(
                    medicine       = medicine,
                    batch_number   = request.POST.get('batch_number', '').strip(),
                    expiry_date    = request.POST.get('expiry_date') or None,
                    quantity       = init_qty,
                    purchase_price = request.POST.get('purchase_price') or 0,
                    supplier_id    = request.POST.get('supplier') or None,
                    created_by     = request.user,
                )
                StockMovement.objects.create(
                    medicine=medicine, batch=batch,
                    movement_type='in',
                    quantity=init_qty,
                    quantity_before=0, quantity_after=init_qty,
                    batch_number=batch.batch_number,
                    expiry_date=batch.expiry_date,
                    purchase_price=batch.purchase_price,
                    notes='Initial stock entry',
                    performed_by=request.user,
                )
            messages.success(request, f'Medicine "{medicine.medicine_name}" added successfully.')
        except Exception as e:
            messages.error(request, f'Error adding medicine: {e}')
    return redirect('inventory')


@require('inventory', 'edit')
def inventory_edit_view(request, pk):
    """
    Edits product-level fields only (name, dosage, price, reorder level, etc.).
    Batch details are managed via Stock In / Stock Management.
    """
    medicine = get_object_or_404(Medicine, pk=pk)
    if request.method == 'POST':
        try:
            barcode = request.POST.get('barcode', '').strip() or None
            if barcode and Medicine.objects.filter(barcode=barcode).exclude(pk=pk).exists():
                messages.error(request, f'Barcode "{barcode}" already assigned.')
                return redirect('inventory')

            medicine.medicine_name    = request.POST.get('medicine_name', '').strip()
            medicine.generic_name     = request.POST.get('generic_name', '').strip()
            medicine.brand_name       = request.POST.get('brand_name', '').strip()
            medicine.category_id      = request.POST.get('category') or None
            medicine.dosage_form      = request.POST.get('dosage_form', 'tablet')
            medicine.strength         = request.POST.get('strength', '').strip()
            medicine.manufacturer     = request.POST.get('manufacturer', '').strip()
            medicine.supplier_id      = request.POST.get('supplier') or None
            medicine.barcode          = barcode
            medicine.selling_price    = request.POST.get('selling_price') or 0
            medicine.reorder_level    = int(request.POST.get('reorder_level') or 10)
            medicine.location_id      = request.POST.get('location') or None
            medicine.is_s2_regulated  = request.POST.get('is_s2_regulated') == '1'
            medicine.save()
            messages.success(request, f'Medicine "{medicine.medicine_name}" updated.')
        except Exception as e:
            messages.error(request, f'Error: {e}')
    return redirect('inventory')


@require(level='admin')
def inventory_delete_view(request, pk):
    if request.method == 'POST':
        medicine = get_object_or_404(Medicine, pk=pk)
        name = medicine.medicine_name
        medicine.is_active = False
        medicine.save()
        messages.success(request, f'Medicine "{name}" removed.')
    return redirect('inventory')


@require(('inventory', 'stock_management', 'dispensing'), ajax=True)
def inventory_barcode_lookup(request):
    barcode = request.GET.get('barcode', '').strip()
    if not barcode:
        return JsonResponse({'found': False})
    try:
        med = Medicine.objects.get(barcode=barcode, is_active=True)
        from django.urls import reverse
        highlight_url = reverse('inventory') + f'?highlight={med.pk}&search={barcode}'
        return JsonResponse({
            'found':          True,
            'pk':             med.pk,
            'medicine_name':  med.medicine_name,
            'generic_name':   med.generic_name,
            'strength':       med.strength,
            'dosage_form':    med.dosage_form,
            'stock_quantity': med.stock_quantity,
            'stock_status':   med.stock_status,
            'highlight_url':  highlight_url,
        })
    except Medicine.DoesNotExist:
        return JsonResponse({'found': False})


@require('inventory', 'edit', ajax=True)
def inventory_stock_adjust(request, pk):
    """Quick +/- stock adjust from inventory page (creates a new batch for 'in')."""
    if request.method == 'POST':
        try:
            data      = json.loads(request.body)
            medicine  = get_object_or_404(Medicine, pk=pk)
            move_type = data.get('type', 'in')
            qty       = int(data.get('quantity', 0))
            notes     = data.get('notes', '')
            if qty <= 0:
                return JsonResponse({'error': 'Quantity must be > 0.'}, status=400)

            old_qty = medicine.stock_quantity

            if move_type == 'out':
                _deduct_fefo(medicine, qty, request.user,
                             movement_type='out', notes=notes)
            else:
                # Create a new batch with no expiry/batch info for quick add
                batch = MedicineStock.objects.create(
                    medicine=medicine, quantity=qty,
                    created_by=request.user,
                )
                StockMovement.objects.create(
                    medicine=medicine, batch=batch,
                    movement_type='in', quantity=qty,
                    quantity_before=old_qty, quantity_after=medicine.stock_quantity,
                    notes=notes, performed_by=request.user,
                )

            return JsonResponse({
                'success':      True,
                'new_quantity': medicine.stock_quantity,
                'stock_status': medicine.stock_status,
            })
        except ValueError as e:
            return JsonResponse({'error': str(e)}, status=400)
        except Exception as e:
            return JsonResponse({'error': str(e)}, status=400)
    return JsonResponse({'error': 'Invalid method.'}, status=405)


# ─────────────────────────────────────────────────────────────────────────────
# MEDICINE SEARCH — AJAX  (used by stock management & dispensing)
# ─────────────────────────────────────────────────────────────────────────────

@require(('inventory', 'stock_management', 'dispensing'), ajax=True)
def medicine_search_ajax(request):
    q  = request.GET.get('q', '').strip()
    qs = Medicine.objects.filter(is_active=True).prefetch_related('batches')
    if q:
        qs = qs.filter(
            Q(medicine_name__icontains=q) |
            Q(generic_name__icontains=q)  |
            Q(brand_name__icontains=q)    |
            Q(barcode__icontains=q)       |
            Q(strength__icontains=q)
        ).distinct()
    qs = qs.select_related('supplier')[:30]
    results = []
    for m in qs:
        # Earliest non-expired active batch for display
        fefo_batch = m.batches.filter(quantity__gt=0).order_by(
            F('expiry_date').asc(nulls_last=True), 'received_at'
        ).first()
        results.append({
            'pk':             m.pk,
            'is_s2_regulated': m.is_s2_regulated,
            'label':          f"{m.medicine_name} {m.strength} ({m.get_dosage_form_display()})",
            'medicine_name':  m.medicine_name,
            'generic_name':   m.generic_name,
            'strength':       m.strength,
            'dosage_form':    m.get_dosage_form_display(),
            'stock_quantity': m.stock_quantity,
            'stock_status':   m.stock_status,
            'reorder_level':  m.reorder_level,
            'supplier_id':    m.supplier_id or '',
            'supplier_name':  m.supplier.name if m.supplier else '',
            # From the FEFO batch (for display in stock management info card)
            'batch_number':   fefo_batch.batch_number if fefo_batch else '',
            'expiry_date':    str(fefo_batch.expiry_date) if fefo_batch and fefo_batch.expiry_date else '',
            'purchase_price': str(fefo_batch.purchase_price) if fefo_batch else '0.00',
            'selling_price':  str(m.selling_price),
            'barcode':        m.barcode or '',
        })
    return JsonResponse({'results': results})


# ─────────────────────────────────────────────────────────────────────────────
# STOCK MANAGEMENT
# ─────────────────────────────────────────────────────────────────────────────

@require('stock_management')
def stock_management_view(request):
    today = timezone.now().date()

    date_from   = request.GET.get('date_from', '')
    date_to     = request.GET.get('date_to', '')
    med_filter  = request.GET.get('medicine', '')
    user_filter = request.GET.get('user', '')
    type_filter = request.GET.get('type', '')
    per_page    = request.GET.get('per_page', '15')

    try:
        per_page = int(per_page)
        if per_page not in [10, 15, 25, 50, 100]:
            per_page = 15
    except ValueError:
        per_page = 15

    movements_qs = StockMovement.objects.select_related(
        'medicine', 'performed_by', 'supplier', 'batch').all()

    if date_from:
        movements_qs = movements_qs.filter(created_at__date__gte=date_from)
    if date_to:
        movements_qs = movements_qs.filter(created_at__date__lte=date_to)
    if med_filter:
        movements_qs = movements_qs.filter(medicine_id=med_filter)
    if user_filter:
        movements_qs = movements_qs.filter(performed_by_id=user_filter)
    if type_filter:
        movements_qs = movements_qs.filter(movement_type=type_filter)

    paginator   = Paginator(movements_qs, per_page)
    page_number = request.GET.get('page', 1)
    try:
        page_obj = paginator.page(page_number)
    except (EmptyPage, PageNotAnInteger):
        page_obj = paginator.page(1)

    today_qs        = StockMovement.objects.filter(created_at__date=today)
    total_in_today  = today_qs.filter(movement_type__in=('in', 'return')).aggregate(
                          t=Sum('quantity'))['t'] or 0
    total_out_today = abs(today_qs.filter(movement_type__in=('out', 'expired', 'damaged')).aggregate(
                          t=Sum('quantity'))['t'] or 0)
    total_adj_today = today_qs.filter(movement_type__in=('adjust', 'audit')).count()
    total_mv_today  = today_qs.count()

    context = {
        'suppliers':      Supplier.objects.filter(is_active=True),
        'page_obj':       page_obj,
        'movements':      page_obj.object_list,
        'paginator':      paginator,
        'per_page':       per_page,
        'per_page_options': [10, 15, 25, 50, 100],
        'movement_types': StockMovement.MOVEMENT_TYPES,
        'date_from':      date_from,
        'date_to':        date_to,
        'med_filter':     med_filter,
        'user_filter':    user_filter,
        'type_filter':    type_filter,
        'all_medicines':  Medicine.objects.filter(is_active=True).order_by('medicine_name'),
        'all_users':      User.objects.filter(is_active=True).order_by('first_name'),
        'total_in_today':          total_in_today,
        'total_out_today':         total_out_today,
        'total_adjustments_today': total_adj_today,
        'total_movements_today':   total_mv_today,
        'today': today,
    }
    return render(request, 'stock_management.html', context)


@require('stock_management', 'edit')
def stock_in_view(request):
    """
    Records a new delivery. Always creates a new MedicineStock batch.
    If a batch with the same batch_number already exists for this medicine,
    it adds to that existing batch instead of creating a duplicate.
    """
    if request.method == 'POST':
        try:
            medicine_id    = request.POST.get('medicine')
            qty            = int(request.POST.get('quantity', 0))
            supplier_id    = request.POST.get('supplier') or None
            batch_number   = request.POST.get('batch_number', '').strip()
            expiry_date    = request.POST.get('expiry_date') or None
            purchase_price = request.POST.get('purchase_price') or 0
            reference_no   = request.POST.get('reference_no', '').strip()
            notes          = request.POST.get('notes', '').strip()

            if not medicine_id:
                messages.error(request, 'Please select a medicine.')
                return redirect('stock_management')
            if qty <= 0:
                messages.error(request, 'Quantity must be greater than zero.')
                return redirect('stock_management')

            medicine = get_object_or_404(Medicine, pk=medicine_id, is_active=True)
            old_total = medicine.stock_quantity

            # Try to match existing batch by batch_number (if provided)
            batch = None
            if batch_number:
                batch = (medicine.batches.filter(batch_number=batch_number)
                         .filter(Q(location__isnull=True) | Q(location=medicine.location))
                         .first())

            if batch:
                # Update existing batch quantity
                batch.quantity       += qty
                batch.purchase_price  = purchase_price  # update price to latest
                if expiry_date:
                    batch.expiry_date = expiry_date
                if supplier_id:
                    batch.supplier_id = supplier_id
                batch.save()
            else:
                # Create new batch
                batch = MedicineStock.objects.create(
                    medicine       = medicine,
                    batch_number   = batch_number,
                    expiry_date    = expiry_date,
                    quantity       = qty,
                    purchase_price = purchase_price,
                    supplier_id    = supplier_id,
                    created_by     = request.user,
                )

            new_total = medicine.stock_quantity
            StockMovement.objects.create(
                medicine=medicine, batch=batch,
                movement_type='in',
                quantity=qty,
                quantity_before=old_total,
                quantity_after=new_total,
                supplier_id=supplier_id,
                batch_number=batch.batch_number,
                expiry_date=batch.expiry_date,
                purchase_price=purchase_price,
                reference_no=reference_no,
                notes=notes,
                performed_by=request.user,
            )
            messages.success(request,
                f'Stock In: +{qty} units of "{medicine.medicine_name}" '
                f'(Batch: {batch_number or "—"}). '
                f'New total: {new_total}.')
        except Exception as e:
            messages.error(request, f'Error recording Stock In: {e}')
    return redirect('stock_management')


@require('stock_management', 'edit')
def stock_out_view(request):
    """
    Records stock removal using FEFO deduction.
    """
    if request.method == 'POST':
        try:
            medicine_id     = request.POST.get('medicine')
            qty             = int(request.POST.get('quantity', 0))
            move_type       = request.POST.get('movement_type', 'out')
            dispensed_to    = request.POST.get('dispensed_to', '').strip()
            prescription_no = request.POST.get('prescription_no', '').strip()
            reference_no    = request.POST.get('reference_no', '').strip()
            notes           = request.POST.get('notes', '').strip()

            if move_type not in ('out', 'expired', 'damaged'):
                move_type = 'out'
            if not medicine_id:
                messages.error(request, 'Please select a medicine.')
                return redirect('stock_management')
            if qty <= 0:
                messages.error(request, 'Quantity must be greater than zero.')
                return redirect('stock_management')

            medicine = get_object_or_404(Medicine, pk=medicine_id, is_active=True)

            _deduct_fefo(
                medicine, qty, request.user,
                movement_type   = move_type,
                dispensed_to    = dispensed_to,
                prescription_no = prescription_no,
                notes           = notes or reference_no,
            )

            labels = {'out': 'Dispensed', 'expired': 'Expired Removal', 'damaged': 'Damaged'}
            messages.success(request,
                f'{labels.get(move_type, "Stock Out")}: -{qty} units of '
                f'"{medicine.medicine_name}". Remaining: {medicine.stock_quantity}.')
        except ValueError as e:
            messages.error(request, str(e))
        except Exception as e:
            messages.error(request, f'Error recording Stock Out: {e}')
    return redirect('stock_management')


@require('stock_management', 'edit')
def stock_adjust_view(request):
    """
    Manual audit adjustment — sets a specific batch to a new quantity.
    If no batch is specified, adjusts the FEFO (oldest) batch.
    """
    if request.method == 'POST':
        try:
            medicine_id  = request.POST.get('medicine')
            new_total    = int(request.POST.get('new_quantity', 0))
            move_type    = request.POST.get('movement_type', 'adjust')
            reference_no = request.POST.get('reference_no', '').strip()
            notes        = request.POST.get('notes', '').strip()
            batch_id     = request.POST.get('batch_id') or None

            if move_type not in ('adjust', 'audit', 'return'):
                move_type = 'adjust'
            if not medicine_id:
                messages.error(request, 'Please select a medicine.')
                return redirect('stock_management')
            if new_total < 0:
                messages.error(request, 'New quantity cannot be negative.')
                return redirect('stock_management')
            if not notes:
                messages.error(request, 'Please provide a reason for this adjustment.')
                return redirect('stock_management')

            medicine  = get_object_or_404(Medicine, pk=medicine_id, is_active=True)
            old_total = medicine.stock_quantity
            diff      = new_total - old_total

            if batch_id:
                batch = get_object_or_404(MedicineStock, pk=batch_id, medicine=medicine)
            else:
                # Adjust the FEFO batch
                batch = medicine.batches.filter(quantity__gt=0).order_by(
                    F('expiry_date').asc(nulls_last=True), 'received_at'
                ).first()
                if not batch:
                    # All batches empty — create one for positive adjustments
                    if diff > 0:
                        batch = MedicineStock.objects.create(
                            medicine=medicine, quantity=0, created_by=request.user
                        )
                    else:
                        messages.error(request, 'No active batches to adjust.')
                        return redirect('stock_management')

            batch.quantity = max(0, batch.quantity + diff)
            batch.save()

            StockMovement.objects.create(
                medicine=medicine, batch=batch,
                movement_type=move_type,
                quantity=diff,
                quantity_before=old_total,
                quantity_after=medicine.stock_quantity,
                batch_number=batch.batch_number,
                expiry_date=batch.expiry_date,
                reference_no=reference_no,
                notes=notes,
                performed_by=request.user,
            )
            direction = f'+{diff}' if diff >= 0 else str(diff)
            messages.success(request,
                f'Adjustment: "{medicine.medicine_name}" {old_total} → {medicine.stock_quantity} ({direction}).')
        except Exception as e:
            messages.error(request, f'Error recording adjustment: {e}')
    return redirect('stock_management')


@require(('stock_management', 'inventory'), ajax=True)
def stock_medicine_info(request):
    pk = request.GET.get('pk', '')
    if not pk:
        return JsonResponse({'found': False})
    try:
        m = Medicine.objects.select_related('location').prefetch_related('batches').get(pk=pk, is_active=True)
        fefo = m.batches.filter(quantity__gt=0).order_by(
            F('expiry_date').asc(nulls_last=True), 'received_at'
        ).first()

        # All active batches for the batch list in the UI
        batches = [
            {
                'pk':             b.pk,
                'batch_number':   b.batch_number or '—',
                'expiry_date':    str(b.expiry_date) if b.expiry_date else '',
                'quantity':       b.quantity,
                'purchase_price': str(b.purchase_price),
                'supplier_id':    b.supplier_id or '',
                'is_expired':     b.is_expired,
                'is_expiring':    b.is_expiring_soon,
                'location':       (b.location.code if b.location else (m.location.code if m.location else '')),
            }
            for b in m.batches.filter(quantity__gt=0).select_related('location').order_by(
                F('expiry_date').asc(nulls_last=True), 'received_at'
            )
        ]

        return JsonResponse({
            'found':          True,
            'medicine_name':  m.medicine_name,
            'generic_name':   m.generic_name,
            'strength':       m.strength,
            'dosage_form':    m.get_dosage_form_display(),
            'stock_quantity': m.stock_quantity,
            'stock_status':   m.stock_status,
            'reorder_level':  m.reorder_level,
            'supplier_id':    m.supplier_id or '',
            'selling_price':  str(m.selling_price),
            # FEFO batch details
            'batch_number':   fefo.batch_number if fefo else '',
            'expiry_date':    str(fefo.expiry_date) if fefo and fefo.expiry_date else '',
            'purchase_price': str(fefo.purchase_price) if fefo else '0.00',
            'barcode':        m.barcode or '',
            'batches':        batches,
        })
    except Medicine.DoesNotExist:
        return JsonResponse({'found': False})


# ─────────────────────────────────────────────────────────────────────────────
# SUPPLIERS
# ─────────────────────────────────────────────────────────────────────────────

@require('suppliers')
def suppliers_view(request):
    search        = request.GET.get('search', '').strip()
    status_filter = request.GET.get('status', '')
    per_page      = request.GET.get('per_page', '15')
    try:
        per_page = int(per_page)
        if per_page not in [10, 15, 25, 50, 100]:
            per_page = 15
    except ValueError:
        per_page = 15

    qs = Supplier.objects.prefetch_related('medicines').all()

    if search:
        qs = qs.filter(
            Q(name__icontains=search)         |
            Q(contact_name__icontains=search) |
            Q(email__icontains=search)        |
            Q(phone__icontains=search))
    if status_filter == 'active':
        qs = qs.filter(is_active=True)
    elif status_filter == 'inactive':
        qs = qs.filter(is_active=False)

    paginator   = Paginator(qs, per_page)
    page_number = request.GET.get('page', 1)
    try:
        page_obj = paginator.page(page_number)
    except (EmptyPage, PageNotAnInteger):
        page_obj = paginator.page(1)

    context = {
        'suppliers':        page_obj.object_list,
        'page_obj':         page_obj,
        'paginator':        paginator,
        'per_page':         per_page,
        'per_page_options': [10, 15, 25, 50, 100],
        'search':                search,
        'status_filter':         status_filter,
        'total_suppliers':       Supplier.objects.count(),
        'active_suppliers':      Supplier.objects.filter(is_active=True).count(),
        'inactive_suppliers':    Supplier.objects.filter(is_active=False).count(),
        'total_products_supplied': Medicine.objects.filter(
                                       is_active=True, supplier__isnull=False).count(),
    }
    return render(request, 'suppliers.html', context)


@require('suppliers', 'edit')
def supplier_add_view(request):
    if request.method == 'POST':
        name = request.POST.get('name', '').strip()
        if not name:
            messages.error(request, 'Supplier name is required.')
            return redirect('suppliers')
        if Supplier.objects.filter(name__iexact=name).exists():
            messages.error(request, f'Supplier "{name}" already exists.')
            return redirect('suppliers')
        Supplier.objects.create(
            name         = name,
            contact_name = request.POST.get('contact_name', '').strip(),
            phone        = request.POST.get('phone', '').strip(),
            email        = request.POST.get('email', '').strip(),
            address      = request.POST.get('address', '').strip(),
            pdea_permit_no     = request.POST.get('pdea_permit_no', '').strip(),
            pdea_permit_expiry = request.POST.get('pdea_permit_expiry', '').strip() or None,
            s4_license_no      = request.POST.get('s4_license_no', '').strip(),
            s4_license_expiry  = request.POST.get('s4_license_expiry', '').strip() or None,
            notes        = request.POST.get('notes', '').strip(),
        )
        messages.success(request, f'Supplier "{name}" added successfully.')
    return redirect('suppliers')


@require('suppliers', 'edit')
def supplier_edit_view(request, pk):
    supplier = get_object_or_404(Supplier, pk=pk)
    if request.method == 'POST':
        name = request.POST.get('name', '').strip()
        if not name:
            messages.error(request, 'Supplier name is required.')
            return redirect('suppliers')
        if Supplier.objects.filter(name__iexact=name).exclude(pk=pk).exists():
            messages.error(request, f'Another supplier named "{name}" already exists.')
            return redirect('suppliers')
        supplier.name         = name
        supplier.contact_name = request.POST.get('contact_name', '').strip()
        supplier.phone        = request.POST.get('phone', '').strip()
        supplier.email        = request.POST.get('email', '').strip()
        supplier.address      = request.POST.get('address', '').strip()
        supplier.pdea_permit_no     = request.POST.get('pdea_permit_no', '').strip()
        supplier.pdea_permit_expiry = request.POST.get('pdea_permit_expiry', '').strip() or None
        supplier.s4_license_no      = request.POST.get('s4_license_no', '').strip()
        supplier.s4_license_expiry  = request.POST.get('s4_license_expiry', '').strip() or None
        supplier.notes        = request.POST.get('notes', '').strip()
        supplier.is_active    = request.POST.get('is_active', 'true') == 'true'
        supplier.save()
        messages.success(request, f'Supplier "{supplier.name}" updated.')
    return redirect('suppliers')


@require(level='admin')
def supplier_delete_view(request, pk):
    if request.method == 'POST':
        supplier = get_object_or_404(Supplier, pk=pk)
        name = supplier.name
        supplier.delete()
        messages.success(request, f'Supplier "{name}" deleted.')
    return redirect('suppliers')


@require('suppliers', 'edit', ajax=True)
def supplier_toggle_view(request, pk):
    if request.method == 'POST':
        supplier = get_object_or_404(Supplier, pk=pk)
        supplier.is_active = not supplier.is_active
        supplier.save()
        label = 'activated' if supplier.is_active else 'deactivated'
        return JsonResponse({
            'success':   True,
            'is_active': supplier.is_active,
            'message':   f'"{supplier.name}" {label}.',
        })
    return JsonResponse({'error': 'Invalid method.'}, status=405)


@require('suppliers', ajax=True)
def supplier_detail_view(request, pk):
    supplier = get_object_or_404(Supplier, pk=pk)
    medicines = list(
        supplier.medicines.filter(is_active=True).values_list('medicine_name', flat=True)
    )
    history_qs = StockMovement.objects.filter(
        supplier=supplier, movement_type='in'
    ).select_related('medicine').order_by('-created_at')[:20]
    purchase_history = [
        {
            'date':      mv.created_at.strftime('%b %d, %Y'),
            'medicine':  mv.medicine.medicine_name,
            'quantity':  mv.quantity,
            'reference': mv.reference_no or '',
            'notes':     mv.notes or '',
        }
        for mv in history_qs
    ]
    return JsonResponse({
        'pk':               supplier.pk,
        'name':             supplier.name,
        'contact_name':     supplier.contact_name,
        'phone':            supplier.phone,
        'email':            supplier.email,
        'address':          supplier.address,
        'pdea_permit_no':     supplier.pdea_permit_no,
        'pdea_permit_expiry': supplier.pdea_permit_expiry.strftime('%b %d, %Y') if supplier.pdea_permit_expiry else '',
        'pdea_status':        supplier.pdea_status,
        's4_license_no':      supplier.s4_license_no,
        's4_license_expiry':  supplier.s4_license_expiry.strftime('%b %d, %Y') if supplier.s4_license_expiry else '',
        's4_status':          supplier.s4_status,
        'notes':            supplier.notes,
        'is_active':        supplier.is_active,
        'medicines':        medicines,
        'purchase_history': purchase_history,
    })


# ─────────────────────────────────────────────────────────────────────────────
# DOCTORS
# ─────────────────────────────────────────────────────────────────────────────

@require('doctors')
def doctors_view(request):
    search        = request.GET.get('search', '').strip()
    status_filter = request.GET.get('status', '')
    s2_filter     = request.GET.get('s2', '')
    per_page      = request.GET.get('per_page', '15')
    try:
        per_page = int(per_page)
        if per_page not in [10, 15, 25, 50, 100]:
            per_page = 15
    except ValueError:
        per_page = 15

    qs = Doctor.objects.all()

    if search:
        qs = qs.filter(
            Q(first_name__icontains=search)   |
            Q(middle_name__icontains=search)  |
            Q(last_name__icontains=search)    |
            Q(prc_license_no__icontains=search) |
            Q(s2_license_no__icontains=search)  |
            Q(specialization__icontains=search)
        )
    if status_filter == 'active':
        qs = qs.filter(is_active=True)
    elif status_filter == 'inactive':
        qs = qs.filter(is_active=False)

    # S2 filter must be done in Python because s2_license_status is a property
    if s2_filter in ('valid', 'expiring_soon', 'expired', 'none'):
        qs = [d for d in qs if d.s2_license_status == s2_filter]

    today = timezone.now().date()

    # Summary card counts (always from full queryset, not filtered)
    all_doctors     = Doctor.objects.all()
    total_doctors   = all_doctors.count()
    active_doctors  = all_doctors.filter(is_active=True).count()
    # S2 licensed = has a license number recorded
    s2_licensed     = sum(1 for d in all_doctors if d.has_s2_license)
    # Expiring soon = S2 expiry within 30 days (valid, not expired)
    expiring_soon   = sum(1 for d in all_doctors if d.s2_is_expiring_soon)

    paginator   = Paginator(qs, per_page)
    page_number = request.GET.get('page', 1)
    try:
        page_obj = paginator.page(page_number)
    except (EmptyPage, PageNotAnInteger):
        page_obj = paginator.page(1)

    context = {
        'doctors':          page_obj.object_list,
        'page_obj':         page_obj,
        'paginator':        paginator,
        'per_page':         per_page,
        'per_page_options': [10, 15, 25, 50, 100],
        'search':           search,
        'status_filter':    status_filter,
        's2_filter':        s2_filter,
        'total_doctors':    total_doctors,
        'active_doctors':   active_doctors,
        's2_licensed':      s2_licensed,
        'expiring_soon':    expiring_soon,
        'today':            today,
        'can_admin': is_admin(request.user),
    }
    return render(request, 'doctors.html', context)


@require('doctors', 'edit')
def doctor_add_view(request):
    if request.method == 'POST':
        prc_no = request.POST.get('prc_license_no', '').strip()
        if not prc_no:
            messages.error(request, 'PRC License No. is required.')
            return redirect('doctors')
        if Doctor.objects.filter(prc_license_no=prc_no).exists():
            messages.error(request, f'A doctor with PRC License No. "{prc_no}" already exists.')
            return redirect('doctors')

        prc_expiry = request.POST.get('prc_expiry_date', '').strip() or None
        s2_expiry  = request.POST.get('s2_expiry_date', '').strip() or None
        s2_no      = request.POST.get('s2_license_no', '').strip()

        Doctor.objects.create(
            first_name      = request.POST.get('first_name', '').strip(),
            middle_name     = request.POST.get('middle_name', '').strip(),
            last_name       = request.POST.get('last_name', '').strip(),
            specialization  = request.POST.get('specialization', '').strip(),
            prc_license_no  = prc_no,
            prc_expiry_date = prc_expiry,
            s2_license_no   = s2_no,
            s2_expiry_date  = s2_expiry,
            contact_number  = request.POST.get('contact_number', '').strip(),
            clinic_address  = request.POST.get('clinic_address', '').strip(),
        )
        messages.success(request, f'Dr. {request.POST.get("first_name", "")} '
                                   f'{request.POST.get("last_name", "")} added successfully.')
    return redirect('doctors')


@require('doctors', 'edit')
def doctor_edit_view(request, pk):
    doctor = get_object_or_404(Doctor, pk=pk)
    if request.method == 'POST':
        prc_no = request.POST.get('prc_license_no', '').strip()
        if not prc_no:
            messages.error(request, 'PRC License No. is required.')
            return redirect('doctors')
        if Doctor.objects.filter(prc_license_no=prc_no).exclude(pk=pk).exists():
            messages.error(request, f'PRC License No. "{prc_no}" is already assigned to another doctor.')
            return redirect('doctors')

        doctor.first_name      = request.POST.get('first_name', '').strip()
        doctor.middle_name     = request.POST.get('middle_name', '').strip()
        doctor.last_name       = request.POST.get('last_name', '').strip()
        doctor.specialization  = request.POST.get('specialization', '').strip()
        doctor.prc_license_no  = prc_no
        doctor.prc_expiry_date = request.POST.get('prc_expiry_date', '').strip() or None
        doctor.s2_license_no   = request.POST.get('s2_license_no', '').strip()
        doctor.s2_expiry_date  = request.POST.get('s2_expiry_date', '').strip() or None
        doctor.contact_number  = request.POST.get('contact_number', '').strip()
        doctor.clinic_address  = request.POST.get('clinic_address', '').strip()
        doctor.save()
        messages.success(request, f'Dr. {doctor.get_full_name()} updated.')
    return redirect('doctors')


@require(level='admin')
def doctor_delete_view(request, pk):
    if request.method == 'POST':
        doctor = get_object_or_404(Doctor, pk=pk)
        name   = doctor.get_full_name()
        # Soft delete — keep record for dispensing history integrity
        doctor.is_active = False
        doctor.save()
        messages.success(request, f'Dr. {name} has been deactivated.')
    return redirect('doctors')


@require('doctors', 'edit', ajax=True)
def doctor_toggle_view(request, pk):
    if request.method == 'POST':
        doctor = get_object_or_404(Doctor, pk=pk)
        doctor.is_active = not doctor.is_active
        doctor.save()
        label = 'activated' if doctor.is_active else 'deactivated'
        return JsonResponse({
            'success':   True,
            'is_active': doctor.is_active,
            'message':   f'Dr. {doctor.get_full_name()} {label}.',
        })
    return JsonResponse({'error': 'Invalid method.'}, status=405)


# ─────────────────────────────────────────────────────────────────────────────
# DOCTOR AJAX ENDPOINTS
# ─────────────────────────────────────────────────────────────────────────────

@require(('doctors', 'dispensing'), ajax=True)
def doctor_search_ajax(request):
    """
    Typeahead search for the dispensing doctor dropdown.
    Only returns active doctors. Frontend uses this to populate the selector.
    """
    q  = request.GET.get('q', '').strip()
    qs = Doctor.objects.filter(is_active=True)
    if q:
        qs = qs.filter(
            Q(first_name__icontains=q)  |
            Q(last_name__icontains=q)   |
            Q(prc_license_no__icontains=q) |
            Q(s2_license_no__icontains=q)  |
            Q(specialization__icontains=q)
        )
    qs = qs.order_by('last_name', 'first_name')[:30]

    results = []
    for d in qs:
        results.append({
            'pk':              d.pk,
            'label':           f"Dr. {d.get_full_name()}" +
                               (f" — {d.specialization}" if d.specialization else ''),
            'full_name':       d.get_full_name(),
            'specialization':  d.specialization,
            'prc_license_no':  d.prc_license_no,
            's2_license_no':   d.s2_license_no,
            's2_license_status': d.s2_license_status,
            's2_expiry_date':  str(d.s2_expiry_date) if d.s2_expiry_date else '',
            's2_is_valid':     d.s2_is_valid_for_dispensing,
        })
    return JsonResponse({'results': results})


@require(('doctors', 'dispensing'), ajax=True)
def doctor_s2_validate_ajax(request):
    """
    Given a doctor PK, returns whether their S2 license is valid for dispensing.
    Called by the dispensing frontend before allowing checkout when cart
    contains any S2-regulated medicine.

    Response shape:
        { valid: bool, reason: str, status: str, expiry: str }
    """
    pk = request.GET.get('pk', '').strip()
    if not pk:
        return JsonResponse({
            'valid':  False,
            'reason': 'No doctor selected.',
            'status': 'none',
            'expiry': '',
        })
    try:
        doctor = Doctor.objects.get(pk=pk, is_active=True)
    except Doctor.DoesNotExist:
        return JsonResponse({
            'valid':  False,
            'reason': 'Doctor not found or is inactive.',
            'status': 'none',
            'expiry': '',
        })

    status = doctor.s2_license_status
    expiry = str(doctor.s2_expiry_date) if doctor.s2_expiry_date else ''

    reason_map = {
        'none':          f'Dr. {doctor.get_full_name()} has no S2 license on record.',
        'expired':       f'Dr. {doctor.get_full_name()}\'s S2 license expired on {expiry}.',
        'expiring_soon': None,   # valid but frontend can show a warning
        'valid':         None,
    }

    is_valid = doctor.s2_is_valid_for_dispensing
    reason   = reason_map.get(status, 'Unknown status.')

    return JsonResponse({
        'valid':  is_valid,
        'reason': reason or '',
        'status': status,
        'expiry': expiry,
        'name':   doctor.get_full_name(),
    })



# ─────────────────────────────────────────────────────────────────────────────
# USER MANAGEMENT
# ─────────────────────────────────────────────────────────────────────────────

def _other_active_admins(exclude_pk):
    return (User.objects.filter(is_active=True)
            .filter(Q(is_superuser=True) | Q(profile__role='admin'))
            .exclude(pk=exclude_pk).count())


@require(level='admin')
def user_management_view(request):
    search_query = request.GET.get('search', '').strip()
    qs = User.objects.select_related('profile', 'permissions').order_by('-date_joined')
    if search_query:
        qs = qs.filter(
            Q(username__icontains=search_query)   |
            Q(first_name__icontains=search_query) |
            Q(last_name__icontains=search_query)  |
            Q(email__icontains=search_query)
        )
    users = list(qs)
    for u in users:
        u.effective_role = get_role(u)
        u.perms_json = json.dumps(u.permissions.as_dict())

    return render(request, 'user_management.html', {
        'users':          users,
        'search_query':   search_query,
        'total_users':    User.objects.count(),
        'active_users':   User.objects.filter(is_active=True).count(),
        'inactive_users': User.objects.filter(is_active=False).count(),
        'module_choices': UserPermission.MODULE_CHOICES,
    })


def _save_module_perms(request, user_obj):
    perm, _created = UserPermission.objects.get_or_create(user=user_obj)
    for key in UserPermission.module_keys():
        setattr(perm, key, request.POST.get(f'perm_{key}') == 'on')
    perm.save()


@require(level='admin')
def user_add_view(request):
    if request.method == 'POST':
        username    = request.POST.get('username', '').strip()
        first_name  = request.POST.get('first_name', '').strip()
        middle_name = request.POST.get('middle_name', '').strip()
        last_name   = request.POST.get('last_name', '').strip()
        email       = request.POST.get('email', '').strip()
        position    = request.POST.get('position', '').strip()
        password    = request.POST.get('password', '')
        confirm_pw  = request.POST.get('confirm_password', '')
        role        = request.POST.get('role', 'user')
        if role not in ('user', 'staff', 'admin'):
            role = 'user'

        if not username:
            messages.error(request, 'Username is required.')
        elif User.objects.filter(username__iexact=username).exists():
            messages.error(request, f'Username "{username}" is taken.')
        elif not password:
            messages.error(request, 'Password cannot be empty.')
        elif password != confirm_pw:
            messages.error(request, 'Passwords do not match.')
        elif len(password) < 8:
            messages.error(request, 'Password must be at least 8 characters.')
        else:
            with transaction.atomic():
                user = User.objects.create_user(
                    username=username, first_name=first_name,
                    last_name=last_name, email=email, password=password,
                )
                # profile + permission rows are created by the post_save signal
                user.profile.middle_name = middle_name
                user.profile.position    = position
                user.profile.role        = role
                user.profile.save()
                if role != 'admin':
                    _save_module_perms(request, user)
            messages.success(request, f'User "{user.username}" created.')
    return redirect('user_management')


@require(level='admin')
def user_edit_view(request, pk):
    user_obj = get_object_or_404(User, pk=pk)
    if request.method == 'POST':
        role = request.POST.get('role', 'user')
        if role not in ('user', 'staff', 'admin'):
            role = 'user'
        is_active = request.POST.get('is_active') == 'on'
        new_pw    = request.POST.get('password', '')

        stays_admin = user_obj.is_superuser or role == 'admin'
        was_admin   = is_admin(user_obj)

        if user_obj == request.user and (not stays_admin or not is_active):
            messages.error(request, "You can't remove your own admin rights or deactivate yourself.")
            return redirect('user_management')
        if was_admin and (not stays_admin or not is_active) \
                and _other_active_admins(user_obj.pk) == 0:
            messages.error(request, 'At least one active administrator is required.')
            return redirect('user_management')
        if new_pw and len(new_pw) < 8:
            messages.error(request, 'Password must be at least 8 characters.')
            return redirect('user_management')

        with transaction.atomic():
            user_obj.first_name = request.POST.get('first_name', '').strip()
            user_obj.last_name  = request.POST.get('last_name', '').strip()
            user_obj.email      = request.POST.get('email', '').strip()
            user_obj.is_active  = is_active
            if new_pw:
                user_obj.set_password(new_pw)
            user_obj.save()

            profile             = user_obj.profile
            profile.middle_name = request.POST.get('middle_name', '').strip()
            profile.position    = request.POST.get('position', '').strip()
            profile.role        = role
            profile.save()

            if role != 'admin':
                _save_module_perms(request, user_obj)

        messages.success(request, f'User "{user_obj.username}" updated.')
    return redirect('user_management')


@require(level='admin')
def user_delete_view(request, pk):
    if request.method == 'POST':
        user_obj = get_object_or_404(User, pk=pk)
        if user_obj == request.user:
            messages.error(request, 'Cannot delete your own account.')
        elif is_admin(user_obj) and _other_active_admins(user_obj.pk) == 0:
            messages.error(request, 'Cannot delete the last administrator.')
        else:
            username = user_obj.username
            user_obj.delete()
            messages.success(request, f'User "{username}" deleted.')
    return redirect('user_management')


@require(level='admin', ajax=True)
def user_toggle_active_view(request, pk):
    if request.method == 'POST':
        user_obj = get_object_or_404(User, pk=pk)
        if user_obj == request.user:
            return JsonResponse({'error': 'Cannot deactivate your own account.'}, status=400)
        if user_obj.is_active and is_admin(user_obj) and _other_active_admins(user_obj.pk) == 0:
            return JsonResponse({'error': 'Cannot deactivate the last administrator.'}, status=400)
        user_obj.is_active = not user_obj.is_active
        user_obj.save()
        label = 'activated' if user_obj.is_active else 'deactivated'
        return JsonResponse({
            'success':   True,
            'is_active': user_obj.is_active,
            'message':   f'{user_obj.get_full_name() or user_obj.username} {label}.',
        })
    return JsonResponse({'error': 'Invalid method.'}, status=405)


# ─────────────────────────────────────────────────────────────────────────────
# SETTINGS
# ─────────────────────────────────────────────────────────────────────────────

@login_required(login_url='login')
def settings_view(request):
    if request.method == 'POST':
        action = request.POST.get('action')
        if action == 'update_profile':
            request.user.first_name = request.POST.get('first_name', '').strip()
            request.user.last_name  = request.POST.get('last_name', '').strip()
            request.user.email      = request.POST.get('email', '').strip()
            request.user.save()
            request.user.profile.middle_name = request.POST.get('middle_name', '').strip()
            request.user.profile.position    = request.POST.get('position', '').strip()
            request.user.profile.save()
            messages.success(request, 'Profile updated.')
        elif action == 'change_password':
            cur = request.POST.get('current_password', '')
            new = request.POST.get('new_password', '')
            cfm = request.POST.get('confirm_password', '')
            if not request.user.check_password(cur):
                messages.error(request, 'Current password incorrect.')
            elif new != cfm:
                messages.error(request, 'New passwords do not match.')
            elif len(new) < 8:
                messages.error(request, 'Password must be at least 8 characters.')
            else:
                request.user.set_password(new)
                request.user.save()
                updated = authenticate(request, username=request.user.username, password=new)
                if updated:
                    login(request, updated)
                messages.success(request, 'Password changed.')
        return redirect('settings')
    return render(request, 'settings.html', {'user_obj': request.user})


# ─────────────────────────────────────────────────────────────────────────────
# REPORTS
# ─────────────────────────────────────────────────────────────────────────────

@require('reports')
def reports_view(request):
    today              = timezone.now().date()
    expiry_threshold   = today + timedelta(days=90)
    date_from, date_to = _parse_date_range(request, default_days=30)
    active_tab         = request.GET.get('tab', 'inventory')

    all_active = Medicine.objects.filter(is_active=True).prefetch_related('batches')

    total_medicines     = all_active.count()
    low_stock_count     = len([m for m in all_active if m.is_low_stock])
    out_of_stock_count  = len([m for m in all_active if m.is_out_of_stock])
    expired_count       = MedicineStock.objects.filter(
                              medicine__is_active=True, quantity__gt=0,
                              expiry_date__lt=today).count()
    expiring_soon_count = MedicineStock.objects.filter(
                              medicine__is_active=True, quantity__gt=0,
                              expiry_date__gte=today,
                              expiry_date__lte=expiry_threshold).count()

    stock_value = MedicineStock.objects.filter(
        medicine__is_active=True, quantity__gt=0
    ).annotate(
        line_val=ExpressionWrapper(
            F('medicine__selling_price') * F('quantity'),
            output_field=DecimalField()
        )
    ).aggregate(total=Sum('line_val'))['total'] or 0

    current_stock_qs = all_active.select_related('category', 'supplier').order_by('medicine_name')
    low_stock_qs     = [m for m in current_stock_qs if m.is_low_stock]
    expired_qs       = MedicineStock.objects.filter(
                           medicine__is_active=True, quantity__gt=0,
                           expiry_date__lt=today
                       ).select_related('medicine__category', 'medicine__supplier').order_by('expiry_date')
    expiring_qs      = MedicineStock.objects.filter(
                           medicine__is_active=True, quantity__gt=0,
                           expiry_date__gte=today, expiry_date__lte=expiry_threshold
                       ).select_related('medicine__category', 'medicine__supplier').order_by('expiry_date')

    category_stock = list(
        MedicineStock.objects.filter(medicine__is_active=True, quantity__gt=0)
        .values('medicine__category__name')
        .annotate(total=Sum('quantity'))
        .order_by('-total')[:10]
    )
    cat_labels = json.dumps([x['medicine__category__name'] or 'Uncategorized' for x in category_stock])
    cat_values = json.dumps([x['total'] for x in category_stock])

    sales_qs = StockMovement.objects.filter(
        movement_type='out',
        created_at__date__gte=date_from,
        created_at__date__lte=date_to,
    ).select_related('medicine', 'performed_by')

    daily_sales = list(
        sales_qs.annotate(day=TruncDay('created_at'))
        .values('day')
        .annotate(units=Sum(ExpressionWrapper(-F('quantity'), output_field=FloatField())))
        .order_by('day')
    )
    sales_labels = json.dumps([str(d['day'].date()) for d in daily_sales])
    sales_units  = json.dumps([d['units'] for d in daily_sales])

    sales_with_value = sales_qs.annotate(
        rev=ExpressionWrapper(
            -F('quantity') * F('medicine__selling_price'),
            output_field=DecimalField()
        )
    )
    total_revenue      = sales_with_value.aggregate(t=Sum('rev'))['t'] or 0
    total_units_sold   = sales_qs.aggregate(
        t=Sum(ExpressionWrapper(-F('quantity'), output_field=FloatField()))
    )['t'] or 0
    total_transactions = sales_qs.count()

    top_medicines = list(
        sales_qs.values('medicine__medicine_name', 'medicine__strength')
        .annotate(
            units=Sum(ExpressionWrapper(-F('quantity'), output_field=FloatField())),
            revenue=Sum(ExpressionWrapper(
                -F('quantity') * F('medicine__selling_price'), output_field=DecimalField()
            ))
        ).order_by('-units')[:10]
    )
    top_med_labels   = json.dumps([f"{m['medicine__medicine_name']} {m['medicine__strength']}" for m in top_medicines])
    top_med_units    = json.dumps([m['units'] for m in top_medicines])
    top_med_revenues = json.dumps([float(m['revenue'] or 0) for m in top_medicines])

    sales_detail = list(
        sales_qs.annotate(
            rev=ExpressionWrapper(
                -F('quantity') * F('medicine__selling_price'), output_field=DecimalField()
            )
        ).values(
            'created_at', 'medicine__medicine_name', 'medicine__strength',
            'quantity', 'dispensed_to', 'prescription_no', 'reference_no',
            'rev', 'performed_by__first_name', 'performed_by__last_name',
            'performed_by__username'
        ).order_by('-created_at')[:200]
    )

    # COGS: use purchase_price recorded on StockMovement at time of deduction
    cogs_qs = StockMovement.objects.filter(
        movement_type='in',
        created_at__date__gte=date_from,
        created_at__date__lte=date_to,
        purchase_price__isnull=False,
    ).annotate(
        cost=ExpressionWrapper(F('quantity') * F('purchase_price'), output_field=DecimalField())
    )
    total_cogs    = cogs_qs.aggregate(t=Sum('cost'))['t'] or 0
    gross_profit  = float(total_revenue) - float(total_cogs)
    profit_margin = (gross_profit / float(total_revenue) * 100) if total_revenue else 0

    monthly_rev = list(
        StockMovement.objects.filter(movement_type='out')
        .annotate(
            month=TruncMonth('created_at'),
            rev=ExpressionWrapper(-F('quantity') * F('medicine__selling_price'), output_field=DecimalField())
        ).values('month').annotate(total_rev=Sum('rev')).order_by('month')
        .filter(month__date__gte=today - timedelta(days=180))
    )
    monthly_rev_labels = json.dumps([str(m['month'].date()) for m in monthly_rev])
    monthly_rev_values = json.dumps([float(m['total_rev'] or 0) for m in monthly_rev])

    monthly_cost = list(
        StockMovement.objects.filter(movement_type='in', purchase_price__isnull=False)
        .annotate(
            month=TruncMonth('created_at'),
            cost=ExpressionWrapper(F('quantity') * F('purchase_price'), output_field=DecimalField())
        ).values('month').annotate(total_cost=Sum('cost')).order_by('month')
        .filter(month__date__gte=today - timedelta(days=180))
    )
    monthly_cost_labels = json.dumps([str(m['month'].date()) for m in monthly_cost])
    monthly_cost_values = json.dumps([float(m['total_cost'] or 0) for m in monthly_cost])

    profit_by_medicine = list(
        StockMovement.objects.filter(
            movement_type='out',
            created_at__date__gte=date_from,
            created_at__date__lte=date_to,
        ).values('medicine__medicine_name', 'medicine__strength')
        .annotate(
            units_sold=Sum(ExpressionWrapper(-F('quantity'), output_field=FloatField())),
            revenue=Sum(ExpressionWrapper(
                -F('quantity') * F('medicine__selling_price'), output_field=DecimalField()
            )),
            cogs=Sum(ExpressionWrapper(
                -F('quantity') * F('purchase_price'), output_field=DecimalField()
            )),
        ).order_by('-revenue')[:15]
    )
    for row in profit_by_medicine:
        rev  = float(row['revenue'] or 0)
        cost = float(row['cogs'] or 0)
        row['gross_profit'] = rev - cost
        row['margin_pct']   = ((rev - cost) / rev * 100) if rev else 0

    context = {
        'active_tab': active_tab,
        'date_from':  date_from,
        'date_to':    date_to,
        'today':      today,
        'total_medicines': total_medicines, 'low_stock_count': low_stock_count,
        'out_of_stock_count': out_of_stock_count, 'expired_count': expired_count,
        'expiring_soon_count': expiring_soon_count, 'stock_value': stock_value,
        'current_stock_qs': current_stock_qs, 'low_stock_qs': low_stock_qs,
        'expired_qs': expired_qs, 'expiring_qs': expiring_qs,
        'cat_labels': cat_labels, 'cat_values': cat_values,
        'total_revenue': total_revenue, 'total_units_sold': total_units_sold,
        'total_transactions': total_transactions, 'sales_detail': sales_detail,
        'top_medicines': top_medicines, 'sales_labels': sales_labels,
        'sales_units': sales_units, 'top_med_labels': top_med_labels,
        'top_med_units': top_med_units, 'top_med_revenues': top_med_revenues,
        'total_cogs': total_cogs, 'gross_profit': gross_profit,
        'profit_margin': round(profit_margin, 1), 'profit_by_medicine': profit_by_medicine,
        'monthly_rev_labels': monthly_rev_labels, 'monthly_rev_values': monthly_rev_values,
        'monthly_cost_labels': monthly_cost_labels, 'monthly_cost_values': monthly_cost_values,
    }
    return render(request, 'reports.html', context)


# ─────────────────────────────────────────────────────────────────────────────
# EXPORT — EXCEL  (unchanged structure, batch-aware expiry data)
# ─────────────────────────────────────────────────────────────────────────────

@require('reports')
def report_export_excel(request):
    if not OPENPYXL_AVAILABLE:
        messages.error(request, 'openpyxl is not installed. Run: pip install openpyxl')
        return redirect('reports')

    report_type        = request.GET.get('type', 'current_stock')
    date_from, date_to = _parse_date_range(request)
    today              = timezone.now().date()
    expiry_threshold   = today + timedelta(days=90)

    wb = openpyxl.Workbook()
    ws = wb.active

    header_fill  = PatternFill('solid', fgColor='00ADB5')
    header_font  = Font(bold=True, color='FFFFFF', size=11)
    title_font   = Font(bold=True, size=14, color='0D1B2A')
    sub_font     = Font(size=9, color='78909C')
    border_side  = Side(style='thin', color='E0E6ED')
    cell_border  = Border(left=border_side, right=border_side, top=border_side, bottom=border_side)
    center_align = Alignment(horizontal='center', vertical='center')
    left_align   = Alignment(horizontal='left',   vertical='center')

    def style_header_row(row_num, col_count):
        for c in range(1, col_count + 1):
            cell = ws.cell(row=row_num, column=c)
            cell.fill = header_fill; cell.font = header_font
            cell.alignment = center_align; cell.border = cell_border

    def style_data_row(row_num, col_count, alternate=False):
        alt_fill = PatternFill('solid', fgColor='F8FAFC' if alternate else 'FFFFFF')
        for c in range(1, col_count + 1):
            cell = ws.cell(row=row_num, column=c)
            cell.fill = alt_fill; cell.border = cell_border; cell.alignment = left_align

    def write_report_title(title, subtitle=''):
        ws.merge_cells(f'A1:{get_column_letter(10)}1')
        ws['A1'].value = 'MHARSMC — Electronic Pharmacy Inventory System'
        ws['A1'].font  = Font(bold=True, size=16, color='0D1B2A')
        ws['A1'].alignment = center_align
        ws.merge_cells(f'A2:{get_column_letter(10)}2')
        ws['A2'].value = title; ws['A2'].font = title_font; ws['A2'].alignment = center_align
        ws.merge_cells(f'A3:{get_column_letter(10)}3')
        ws['A3'].value = subtitle or f'Generated: {timezone.now().strftime("%B %d, %Y %I:%M %p")}'
        ws['A3'].font  = sub_font; ws['A3'].alignment = center_align
        return 5

    if report_type == 'current_stock':
        ws.title  = 'Current Stock'
        start_row = write_report_title('Current Stock Report')
        headers   = ['#', 'Medicine Name', 'Generic Name', 'Category',
                     'Dosage Form', 'Strength', 'Supplier',
                     'Stock Qty', 'Reorder Level', 'Status']
        ws.append([])
        for col, h in enumerate(headers, 1):
            ws.cell(row=start_row, column=col).value = h
        style_header_row(start_row, len(headers))
        qs = Medicine.objects.filter(is_active=True).select_related(
            'category', 'supplier').prefetch_related('batches').order_by('medicine_name')
        for i, m in enumerate(qs, 1):
            r = start_row + i
            status = 'Out of Stock' if m.is_out_of_stock else \
                     'Low Stock'    if m.is_low_stock    else 'In Stock'
            ws.append([i, m.medicine_name, m.generic_name,
                        m.category.name if m.category else '—',
                        m.get_dosage_form_display(), m.strength,
                        m.supplier.name if m.supplier else '—',
                        m.stock_quantity, m.reorder_level, status])
            style_data_row(r, len(headers), i % 2 == 0)
            status_cell = ws.cell(row=r, column=10)
            status_cell.font = Font(bold=True, color='DC2626' if m.is_out_of_stock else
                                    ('D97706' if m.is_low_stock else '16A34A'))
        col_widths = [5, 28, 25, 18, 14, 12, 22, 10, 12, 12]

    elif report_type == 'low_stock':
        ws.title  = 'Low Stock'
        start_row = write_report_title('Low Stock Report', f'Medicines at or below reorder level — {today}')
        headers   = ['#', 'Medicine Name', 'Generic Name', 'Category',
                     'Strength', 'Stock Qty', 'Reorder Level', 'Deficit', 'Supplier']
        ws.append([])
        for col, h in enumerate(headers, 1):
            ws.cell(row=start_row, column=col).value = h
        style_header_row(start_row, len(headers))
        qs = [m for m in Medicine.objects.filter(is_active=True).select_related(
              'category', 'supplier').prefetch_related('batches').order_by('medicine_name')
              if m.is_low_stock]
        for i, m in enumerate(qs, 1):
            r = start_row + i
            ws.append([i, m.medicine_name, m.generic_name,
                        m.category.name if m.category else '—',
                        m.strength, m.stock_quantity, m.reorder_level,
                        m.reorder_level - m.stock_quantity,
                        m.supplier.name if m.supplier else '—'])
            style_data_row(r, len(headers), i % 2 == 0)
        col_widths = [5, 28, 25, 18, 12, 10, 12, 10, 22]

    elif report_type == 'expired':
        ws.title  = 'Expired Batches'
        start_row = write_report_title('Expired Batches Report', f'As of {today}')
        headers   = ['#', 'Medicine Name', 'Generic Name', 'Category',
                     'Strength', 'Batch Number', 'Expiry Date', 'Qty Remaining', 'Supplier']
        ws.append([])
        for col, h in enumerate(headers, 1):
            ws.cell(row=start_row, column=col).value = h
        style_header_row(start_row, len(headers))
        qs = MedicineStock.objects.filter(
            medicine__is_active=True, quantity__gt=0, expiry_date__lt=today
        ).select_related('medicine__category', 'medicine__supplier').order_by('expiry_date')
        for i, b in enumerate(qs, 1):
            r = start_row + i
            m = b.medicine
            ws.append([i, m.medicine_name, m.generic_name,
                        m.category.name if m.category else '—',
                        m.strength, b.batch_number or '—',
                        str(b.expiry_date), b.quantity,
                        b.supplier.name if b.supplier else (m.supplier.name if m.supplier else '—')])
            style_data_row(r, len(headers), i % 2 == 0)
            ws.cell(row=r, column=7).font = Font(bold=True, color='DC2626')
        col_widths = [5, 28, 25, 18, 12, 16, 14, 10, 22]

    elif report_type == 'expiring':
        ws.title  = 'Expiring Soon'
        start_row = write_report_title('Expiring Soon Report', f'Batches expiring within 90 days — {today}')
        headers   = ['#', 'Medicine Name', 'Generic Name', 'Category',
                     'Strength', 'Batch Number', 'Expiry Date', 'Days Left', 'Qty', 'Supplier']
        ws.append([])
        for col, h in enumerate(headers, 1):
            ws.cell(row=start_row, column=col).value = h
        style_header_row(start_row, len(headers))
        qs = MedicineStock.objects.filter(
            medicine__is_active=True, quantity__gt=0,
            expiry_date__gte=today, expiry_date__lte=expiry_threshold
        ).select_related('medicine__category', 'medicine__supplier').order_by('expiry_date')
        for i, b in enumerate(qs, 1):
            r         = start_row + i
            m         = b.medicine
            days_left = (b.expiry_date - today).days
            ws.append([i, m.medicine_name, m.generic_name,
                        m.category.name if m.category else '—',
                        m.strength, b.batch_number or '—',
                        str(b.expiry_date), days_left, b.quantity,
                        b.supplier.name if b.supplier else (m.supplier.name if m.supplier else '—')])
            style_data_row(r, len(headers), i % 2 == 0)
            ws.cell(row=r, column=8).font = Font(
                bold=True, color='D97706' if days_left <= 30 else '1A2433'
            )
        col_widths = [5, 28, 25, 18, 12, 16, 14, 10, 10, 22]

    elif report_type == 'sales':
        ws.title  = 'Sales Report'
        start_row = write_report_title('Sales Report', f'{date_from} to {date_to}')
        headers   = ['#', 'Date', 'Medicine', 'Strength', 'Qty Dispensed',
                     'Selling Price', 'Revenue', 'Dispensed To', 'Prescription No.', 'Performed By']
        ws.append([])
        for col, h in enumerate(headers, 1):
            ws.cell(row=start_row, column=col).value = h
        style_header_row(start_row, len(headers))
        qs = StockMovement.objects.filter(
            movement_type='out',
            created_at__date__gte=date_from,
            created_at__date__lte=date_to,
        ).select_related('medicine', 'performed_by').order_by('-created_at')
        for i, mv in enumerate(qs, 1):
            r   = start_row + i
            qty = abs(mv.quantity)
            rev = qty * mv.medicine.selling_price
            by  = mv.performed_by.get_full_name() if mv.performed_by else '—'
            ws.append([i, mv.created_at.strftime('%Y-%m-%d %H:%M'),
                        mv.medicine.medicine_name, mv.medicine.strength,
                        qty, float(mv.medicine.selling_price), float(rev),
                        mv.dispensed_to or '—', mv.prescription_no or '—', by])
            style_data_row(r, len(headers), i % 2 == 0)
            ws.cell(row=r, column=7).number_format = '#,##0.00'
            ws.cell(row=r, column=6).number_format = '#,##0.00'
        col_widths = [5, 18, 28, 12, 14, 13, 13, 22, 16, 20]

    elif report_type == 'financial':
        ws.title  = 'Financial Report'
        start_row = write_report_title('Financial Report', f'{date_from} to {date_to}')
        headers   = ['#', 'Medicine', 'Strength', 'Units Sold',
                     'Selling Price', 'Purchase Price', 'Revenue', 'COGS', 'Gross Profit', 'Margin %']
        ws.append([])
        for col, h in enumerate(headers, 1):
            ws.cell(row=start_row, column=col).value = h
        style_header_row(start_row, len(headers))
        qs = StockMovement.objects.filter(
            movement_type='out',
            created_at__date__gte=date_from,
            created_at__date__lte=date_to,
        ).values('medicine__medicine_name', 'medicine__strength',
                 'medicine__selling_price', 'medicine__purchase_price') \
         .annotate(
             units=Sum(ExpressionWrapper(-F('quantity'), output_field=FloatField())),
             rev=Sum(ExpressionWrapper(
                 -F('quantity') * F('medicine__selling_price'), output_field=DecimalField()
             )),
             cost=Sum(ExpressionWrapper(
                 -F('quantity') * F('medicine__purchase_price'), output_field=DecimalField()
             )),
         ).order_by('-rev')
        for i, row in enumerate(qs, 1):
            r      = start_row + i
            rev    = float(row['rev'] or 0)
            cost   = float(row['cost'] or 0)
            profit = rev - cost
            margin = (profit / rev * 100) if rev else 0
            ws.append([i, row['medicine__medicine_name'], row['medicine__strength'],
                        row['units'], float(row['medicine__selling_price']),
                        float(row['medicine__purchase_price']),
                        rev, cost, profit, round(margin, 1)])
            style_data_row(r, len(headers), i % 2 == 0)
            for c in [5, 6, 7, 8, 9]:
                ws.cell(row=r, column=c).number_format = '#,##0.00'
            ws.cell(row=r, column=9).font = Font(
                bold=True, color='16A34A' if profit >= 0 else 'DC2626'
            )
        col_widths = [5, 28, 12, 12, 13, 13, 13, 13, 13, 10]
    else:
        col_widths = [20] * 5

    for i, w in enumerate(col_widths, 1):
        ws.column_dimensions[get_column_letter(i)].width = w
    ws.freeze_panes = ws.cell(row=6, column=1)

    filename = f'MHARSMC_{report_type}_{today}.xlsx'
    response = HttpResponse(
        content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
    )
    response['Content-Disposition'] = f'attachment; filename="{filename}"'
    wb.save(response)
    return response


# ─────────────────────────────────────────────────────────────────────────────
# EXPORT — PDF
# ─────────────────────────────────────────────────────────────────────────────

@require('reports')
def report_export_pdf(request):
    if not REPORTLAB_AVAILABLE:
        messages.error(request, 'reportlab is not installed. Run: pip install reportlab')
        return redirect('reports')

    report_type        = request.GET.get('type', 'current_stock')
    date_from, date_to = _parse_date_range(request)
    today              = timezone.now().date()
    expiry_threshold   = today + timedelta(days=90)

    buffer = io.BytesIO()
    FOLIO  = (8.5 * inch, 13 * inch)
    wide_types = {'current_stock', 'sales', 'financial', 'expiring', 'expired', 'low_stock'}
    pagesize   = landscape(FOLIO) if report_type in wide_types else FOLIO

    doc = SimpleDocTemplate(buffer, pagesize=pagesize,
                            leftMargin=1.5*cm, rightMargin=1.5*cm,
                            topMargin=1.5*cm, bottomMargin=1.5*cm)
    styles   = getSampleStyleSheet()
    elements = []

    TEAL    = colors.HexColor('#00ADB5')
    DARK    = colors.HexColor('#0D1B2A')
    MUTED   = colors.HexColor('#78909C')
    RED     = colors.HexColor('#DC2626')
    ORANGE  = colors.HexColor('#D97706')
    GREEN   = colors.HexColor('#16A34A')
    ALT_ROW = colors.HexColor('#EEF2F7')

    title_style = ParagraphStyle('Title', parent=styles['Normal'],
                                 fontSize=12, textColor=DARK,
                                 fontName='Helvetica-Bold', alignment=TA_CENTER, spaceAfter=2)
    sub_style   = ParagraphStyle('Sub', parent=styles['Normal'],
                                 fontSize=9, textColor=MUTED, alignment=TA_CENTER, spaceAfter=4)
    body_style  = ParagraphStyle('Body', parent=styles['Normal'], fontSize=8, textColor=DARK)

    def make_table_style(num_rows, num_cols):
        return TableStyle([
            ('BACKGROUND',  (0,0),  (-1,0),  TEAL),
            ('TEXTCOLOR',   (0,0),  (-1,0),  colors.white),
            ('FONTNAME',    (0,0),  (-1,0),  'Helvetica-Bold'),
            ('FONTSIZE',    (0,0),  (-1,0),  8),
            ('ALIGN',       (0,0),  (-1,-1), 'CENTER'),
            ('VALIGN',      (0,0),  (-1,-1), 'MIDDLE'),
            ('FONTNAME',    (0,1),  (-1,-1), 'Helvetica'),
            ('FONTSIZE',    (0,1),  (-1,-1), 7.5),
            ('ROWBACKGROUNDS', (0,1), (-1,-1), [colors.white, ALT_ROW]),
            ('GRID',        (0,0),  (-1,-1), 0.3, colors.HexColor('#E0E6ED')),
            ('BOTTOMPADDING',(0,0), (-1,-1), 4),
            ('TOPPADDING',  (0,0),  (-1,-1), 4),
            ('LEFTPADDING', (0,0),  (-1,-1), 5),
            ('RIGHTPADDING',(0,0),  (-1,-1), 5),
        ])

    elements.append(Paragraph('MHARSMC — Electronic Pharmacy Inventory System', title_style))
    elements.append(HRFlowable(width='100%', thickness=2, color=TEAL, spaceAfter=6))

    report_titles = {
        'current_stock': 'Current Stock Report',
        'low_stock':     'Low Stock Report',
        'expired':       'Expired Batches Report',
        'expiring':      'Expiring Soon Report',
        'sales':         f'Sales Report: {date_from} to {date_to}',
        'financial':     f'Financial Report: {date_from} to {date_to}',
    }
    elements.append(Paragraph(report_titles.get(report_type, 'Report'), title_style))
    elements.append(Paragraph(
        f'Generated: {timezone.now().strftime("%B %d, %Y %I:%M %p")} | '
        f'Mayor Hilarion A. Ramiro Sr. Medical Center', sub_style
    ))
    elements.append(Spacer(1, 0.4*cm))

    if report_type == 'current_stock':
        qs   = Medicine.objects.filter(is_active=True).select_related(
                   'category', 'supplier').prefetch_related('batches').order_by('medicine_name')
        data = [['#', 'Medicine Name', 'Generic', 'Category',
                 'Form', 'Strength', 'Stock', 'Reorder', 'Status']]
        ts   = make_table_style(qs.count() + 1, 9)
        for i, m in enumerate(qs, 1):
            status = 'Out' if m.is_out_of_stock else 'Low' if m.is_low_stock else 'OK'
            data.append([i, m.medicine_name, m.generic_name,
                          m.category.name if m.category else '—',
                          m.get_dosage_form_display(), m.strength,
                          m.stock_quantity, m.reorder_level, status])
            color = RED if m.is_out_of_stock else (ORANGE if m.is_low_stock else GREEN)
            ts.add('TEXTCOLOR', (8, i), (8, i), color)
            ts.add('FONTNAME',  (8, i), (8, i), 'Helvetica-Bold')
        col_widths = [1*cm, 5.5*cm, 4.5*cm, 3*cm, 2.5*cm, 2.5*cm, 1.8*cm, 2*cm, 1.8*cm]

    elif report_type == 'low_stock':
        qs   = [m for m in Medicine.objects.filter(is_active=True).select_related(
                    'category', 'supplier').prefetch_related('batches')
                if m.is_low_stock]
        data = [['#', 'Medicine', 'Generic', 'Category',
                 'Strength', 'Stock', 'Reorder', 'Deficit', 'Supplier']]
        ts   = make_table_style(len(qs) + 1, 9)
        for i, m in enumerate(qs, 1):
            data.append([i, m.medicine_name, m.generic_name,
                          m.category.name if m.category else '—',
                          m.strength, m.stock_quantity, m.reorder_level,
                          m.reorder_level - m.stock_quantity,
                          m.supplier.name if m.supplier else '—'])
        col_widths = [1*cm, 5.5*cm, 4.5*cm, 3*cm, 2.5*cm, 1.8*cm, 2*cm, 2*cm, 4*cm]

    elif report_type == 'expired':
        qs   = MedicineStock.objects.filter(
                   medicine__is_active=True, quantity__gt=0, expiry_date__lt=today
               ).select_related('medicine__category', 'medicine__supplier').order_by('expiry_date')
        data = [['#', 'Medicine', 'Generic', 'Category',
                 'Strength', 'Batch', 'Expiry Date', 'Qty', 'Supplier']]
        ts   = make_table_style(qs.count() + 1, 9)
        for i, b in enumerate(qs, 1):
            m = b.medicine
            data.append([i, m.medicine_name, m.generic_name,
                          m.category.name if m.category else '—',
                          m.strength, b.batch_number or '—',
                          str(b.expiry_date), b.quantity,
                          b.supplier.name if b.supplier else (m.supplier.name if m.supplier else '—')])
            ts.add('TEXTCOLOR', (6, i), (6, i), RED)
            ts.add('FONTNAME',  (6, i), (6, i), 'Helvetica-Bold')
        col_widths = [1*cm, 5.5*cm, 4.5*cm, 2.5*cm, 2.5*cm, 2.5*cm, 2.8*cm, 1.5*cm, 4*cm]

    elif report_type == 'expiring':
        qs   = MedicineStock.objects.filter(
                   medicine__is_active=True, quantity__gt=0,
                   expiry_date__gte=today, expiry_date__lte=expiry_threshold
               ).select_related('medicine__category', 'medicine__supplier').order_by('expiry_date')
        data = [['#', 'Medicine', 'Generic', 'Category',
                 'Strength', 'Batch', 'Expiry Date', 'Days Left', 'Qty']]
        ts   = make_table_style(qs.count() + 1, 9)
        for i, b in enumerate(qs, 1):
            m         = b.medicine
            days_left = (b.expiry_date - today).days
            data.append([i, m.medicine_name, m.generic_name,
                          m.category.name if m.category else '—',
                          m.strength, b.batch_number or '—',
                          str(b.expiry_date), days_left, b.quantity])
            color = RED if days_left <= 30 else ORANGE
            ts.add('TEXTCOLOR', (7, i), (7, i), color)
            ts.add('FONTNAME',  (7, i), (7, i), 'Helvetica-Bold')
        col_widths = [1*cm, 5.5*cm, 4.5*cm, 2.5*cm, 2.5*cm, 2.5*cm, 2.8*cm, 2*cm, 2*cm]

    elif report_type == 'sales':
        qs   = StockMovement.objects.filter(
                   movement_type='out',
                   created_at__date__gte=date_from,
                   created_at__date__lte=date_to,
               ).select_related('medicine', 'performed_by').order_by('-created_at')
        data = [['#', 'Date', 'Medicine', 'Strength',
                 'Qty', 'Sell Price', 'Revenue', 'Dispensed To', 'By']]
        ts        = make_table_style(qs.count() + 1, 9)
        total_rev = 0
        for i, mv in enumerate(qs, 1):
            qty = abs(mv.quantity)
            rev = float(qty * mv.medicine.selling_price)
            total_rev += rev
            by  = mv.performed_by.get_full_name() if mv.performed_by else '—'
            data.append([i, mv.created_at.strftime('%m/%d/%Y'),
                          mv.medicine.medicine_name, mv.medicine.strength,
                          qty, f'₱{mv.medicine.selling_price:,.2f}',
                          f'₱{rev:,.2f}', mv.dispensed_to or '—', by])
        data.append(['', '', '', '', '', 'TOTAL', f'₱{total_rev:,.2f}', '', ''])
        ts.add('FONTNAME',   (0, len(data)-1), (-1, len(data)-1), 'Helvetica-Bold')
        ts.add('BACKGROUND', (0, len(data)-1), (-1, len(data)-1), ALT_ROW)
        ts.add('TEXTCOLOR',  (6, len(data)-1), (6,  len(data)-1), GREEN)
        col_widths = [1*cm, 2.5*cm, 5.5*cm, 2.5*cm, 1.5*cm, 2.5*cm, 2.5*cm, 4*cm, 3.5*cm]

    elif report_type == 'financial':
        qs = StockMovement.objects.filter(
            movement_type='out',
            created_at__date__gte=date_from,
            created_at__date__lte=date_to,
        ).values('medicine__medicine_name', 'medicine__strength',
                 'medicine__selling_price', 'medicine__purchase_price') \
         .annotate(
             units=Sum(ExpressionWrapper(-F('quantity'), output_field=FloatField())),
             rev=Sum(ExpressionWrapper(
                 -F('quantity') * F('medicine__selling_price'), output_field=DecimalField()
             )),
             cost=Sum(ExpressionWrapper(
                 -F('quantity') * F('medicine__purchase_price'), output_field=DecimalField()
             )),
         ).order_by('-rev')
        data = [['#', 'Medicine', 'Strength', 'Units',
                 'Revenue', 'COGS', 'Gross Profit', 'Margin %']]
        ts   = make_table_style(qs.count() + 1, 8)
        for i, row in enumerate(qs, 1):
            rev    = float(row['rev'] or 0)
            cost   = float(row['cost'] or 0)
            profit = rev - cost
            margin = (profit / rev * 100) if rev else 0
            data.append([i, row['medicine__medicine_name'], row['medicine__strength'],
                          row['units'], f'₱{rev:,.2f}', f'₱{cost:,.2f}',
                          f'₱{profit:,.2f}', f'{margin:.1f}%'])
            color = GREEN if profit >= 0 else RED
            ts.add('TEXTCOLOR', (6, i), (6, i), color)
            ts.add('FONTNAME',  (6, i), (6, i), 'Helvetica-Bold')
        col_widths = [1*cm, 6*cm, 2.5*cm, 2*cm, 3*cm, 3*cm, 3.5*cm, 2.5*cm]
    else:
        data       = [['No data']]
        ts         = make_table_style(1, 1)
        col_widths = None

    if len(data) > 1 or data[0][0] != 'No data':
        table = Table(data, colWidths=col_widths, repeatRows=1)
        table.setStyle(ts)
        elements.append(table)
    else:
        elements.append(Paragraph('No data available for this report.', body_style))

    elements.append(Spacer(1, 0.5*cm))
    elements.append(Paragraph(
        f'— End of Report — | MHARSMC Electronic Pharmacy Inventory System | {timezone.now().strftime("%B %d, %Y")}',
        sub_style
    ))

    doc.build(elements)
    buffer.seek(0)
    filename = f'MHARSMC_{report_type}_{today}.pdf'
    response = HttpResponse(content_type='application/pdf')
    response['Content-Disposition'] = f'attachment; filename="{filename}"'
    response.write(buffer.read())
    return response




AVG_PERIOD_CHOICES = [('monthly', 'Monthly'), ('quarterly', 'Quarterly'), ('yearly', 'Yearly')]
QUARTER_CHOICES = [
    (1, '1st Quarter (Jan–Mar)'),
    (2, '2nd Quarter (Apr–Jun)'),
    (3, '3rd Quarter (Jul–Sep)'),
    (4, '4th Quarter (Oct–Dec)'),
]
 
 
def _consumption_by_month(start, end):
    """
    {medicine_id: {(year, month): consumed}} for start <= created_at < end.
    Uses the same definition of "consumed" as the monthly report:
    dispensed items + expired/damaged removals (the 'out' movement is NOT
    counted again because dispensing already writes it).
    """
    out = {}
 
    def add(med_id, dt, qty):
        key = (dt.year, dt.month)
        bucket = out.setdefault(med_id, {})
        bucket[key] = bucket.get(key, 0) + qty
 
    dispensed = (
        DispensingItem.objects
        .filter(dispensing__created_at__gte=start, dispensing__created_at__lt=end)
        .annotate(m=TruncMonth('dispensing__created_at'))
        .values('medicine_id', 'm')
        .annotate(t=Sum('quantity'))
    )
    for r in dispensed:
        add(r['medicine_id'], r['m'], r['t'] or 0)
 
    manual = (
        StockMovement.objects
        .filter(movement_type__in=['expired', 'damaged'],
                created_at__gte=start, created_at__lt=end)
        .annotate(m=TruncMonth('created_at'))
        .values('medicine_id', 'm')
        .annotate(t=Sum('quantity'))
    )
    for r in manual:
        add(r['medicine_id'], r['m'], abs(r['t'] or 0))
 
    return out
 
 
def average_consumption_context(request, submitted_by, position):
    today = timezone.localdate()
 
    period = request.GET.get('period', 'monthly')
    if period not in dict(AVG_PERIOD_CHOICES):
        period = 'monthly'
 
    try:
        year    = int(request.GET.get('year', today.year))
        month   = int(request.GET.get('month', today.month))
        quarter = int(request.GET.get('quarter', (today.month - 1) // 3 + 1))
        if not (1 <= month <= 12 and 1 <= quarter <= 4):
            raise ValueError
    except (ValueError, TypeError):
        year, month, quarter = today.year, today.month, (today.month - 1) // 3 + 1
 
    # ── Period definition ────────────────────────────────────────────────────
    if period == 'monthly':
        first_month, n_months = month, 1
        label = f'{calendar.month_name[month]} {year}'
    elif period == 'quarterly':
        first_month, n_months = (quarter - 1) * 3 + 1, 3
        label = (f'{dict(QUARTER_CHOICES)[quarter]} {year}')
    else:
        first_month, n_months = 1, 12
        label = f'January–December {year}'
 
    end_idx   = first_month - 1 + n_months          # 0-based month index from Jan of `year`
    end_year  = year + end_idx // 12
    end_month = end_idx % 12 + 1
    start = timezone.make_aware(datetime(year, first_month, 1))
    end   = timezone.make_aware(datetime(end_year, end_month, 1))
 
    months = [(year, first_month + i) for i in range(n_months)]
    month_labels = [calendar.month_abbr[m] for _, m in months]
 
    # ── Divisors (cap at today when the period is still running) ─────────────
    total_days = (end.date() - start.date()).days
    if today < start.date():
        days_counted = 0
    elif today < end.date():
        days_counted = (today - start.date()).days + 1
    else:
        days_counted = total_days
    is_partial = days_counted < total_days
    months_counted = round(n_months * days_counted / total_days, 2) if total_days else 0
    day_div   = days_counted or 1
    month_div = months_counted or 1


    stock_map = dict(
        MedicineStock.objects
        .filter(medicine__is_active=True, quantity__gt=0)
        .order_by()
        .values_list('medicine_id')
        .annotate(t=Sum('quantity'))
    )
 
    # ── Rows ─────────────────────────────────────────────────────────────────
    data = _consumption_by_month(start, end)
    rows = []
    month_totals = [0] * n_months
    for med in Medicine.objects.filter(is_active=True).order_by('medicine_name'):
        per_month = data.get(med.pk, {})
        vals  = [per_month.get(k, 0) for k in months]
        total = sum(vals)
        for i, v in enumerate(vals):
            month_totals[i] += v
        avg_month = round(total / month_div, 2)
        stock     = stock_map.get(med.pk, 0)
        rows.append({
            'item_code':       med.barcode or f"MED-{med.pk:04d}",
            'description':     f"{med.medicine_name} {med.strength}",
            'unit_of_measure': med.dosage_form,
            'months':          vals,
            'total':           total,
            'avg_month':       avg_month,
            'avg_day':         round(total / day_div, 2),
            'stock':           stock,
            'months_left':     round(stock / avg_month, 1) if avg_month else None,
        })
 
    total_consumed = sum(r['total'] for r in rows)
    total_stock    = sum(r['stock'] for r in rows)
 
    return {
        'active_tab':       'average',
        'period':           period,
        'period_choices':   AVG_PERIOD_CHOICES,
        'period_title':     dict(AVG_PERIOD_CHOICES)[period],
        'period_label':     label,
        'quarter_choices':  QUARTER_CHOICES,
        'selected_quarter': quarter,
        'selected_month':   month,
        'selected_year':    year,
        'month_choices':    [(i, calendar.month_name[i]) for i in range(1, 13)],
        'year_choices':     list(range(today.year - 3, today.year + 2)),
        'rows':             rows,
        'show_breakdown':   n_months > 1,
        'month_labels':     month_labels,
        'month_totals':     month_totals,
        'total_consumed':   total_consumed,
        'total_stock':      total_stock,
        'total_avg_month':  round(total_consumed / month_div, 2),
        'total_avg_day':    round(total_consumed / day_div, 2),
        'days_counted':     days_counted,
        'months_counted':   months_counted,
        'is_partial':       is_partial,
        'submitted_by':     submitted_by,
        'position':         position,
    }



@require('consumption_report')
def consumption_report(request):
    # Helper function to format name with middle initial
    def get_formatted_name(user):
        """Return formatted name: FIRST MIDDLEINITIAL. LAST (all caps)"""
        first_name = user.first_name or ""
        last_name = user.last_name or ""
        
        # Get middle name from profile
        middle_name = ""
        if hasattr(user, 'profile'):
            middle_name = user.profile.middle_name or ""
        
        # Get middle initial
        middle_initial = f"{middle_name[0].upper()}." if middle_name else ""
        
        # Build the formatted name
        if middle_initial:
            formatted = f"{first_name.upper()} {middle_initial} {last_name.upper()}"
        else:
            formatted = f"{first_name.upper()} {last_name.upper()}"
        
        return formatted.strip() or user.username.upper()

    # ── Average Consumption tab: return early, skip the monthly calculation ──
    if request.GET.get('tab') == 'average':
        submitted_by = get_formatted_name(request.user)
        position = request.user.profile.position if hasattr(request.user, 'profile') else '—'
        return render(request, 'consumption_report.html',
                      average_consumption_context(request, submitted_by, position))

    # ── 1. Resolve selected month/year ────────────────────────────────────────
    today = timezone.now().date()
    try:
        year  = int(request.GET.get('year',  today.year))
        month = int(request.GET.get('month', today.month))
        if not (1 <= month <= 12):
            raise ValueError
    except (ValueError, TypeError):
        year, month = today.year, today.month
 
    # Use timezone-aware datetimes to avoid naive datetime warnings
    period_start = timezone.make_aware(timezone.datetime(year, month, 1))
    if month == 12:
        period_end = timezone.make_aware(timezone.datetime(year + 1, 1, 1))
    else:
        period_end = timezone.make_aware(timezone.datetime(year, month + 1, 1))
 
    # ── 2. All active medicines (ordered by medicine_name) ────────────────────
    medicines = Medicine.objects.filter(is_active=True).select_related('category')
 
    rows = []
    for med in medicines:
 
        # ── Previous Balance ─────────────────────────────────────────────────
        # The quantity_after of the LAST stock movement BEFORE the period start.
        last_movement = (
            StockMovement.objects
            .filter(medicine=med, created_at__lt=period_start)
            .order_by('-created_at')
            .first()
        )
        if last_movement:
            previous_balance = last_movement.quantity_after
        else:
            # No movements before this month — reconstruct from current stock
            # by reversing movements that happened during or after the period.
            movements_since = StockMovement.objects.filter(
                medicine=med, created_at__gte=period_start
            )
            delta = sum(m.quantity for m in movements_since)  # signed integer
            previous_balance = max(med.stock_quantity - delta, 0)
 
        # ── Received during Month ────────────────────────────────────────────
        received_qs = StockMovement.objects.filter(
            medicine=med,
            movement_type__in=['in', 'return'],
            created_at__gte=period_start,
            created_at__lt=period_end,
        ).aggregate(total=Sum('quantity'))
        received = received_qs['total'] or 0
 
        # ── Consumed during Month ────────────────────────────────────────────
        # a) Dispensing — DispensingItem is the source of truth for dispensed qty.
        dispensed_qs = DispensingItem.objects.filter(
            medicine=med,
            dispensing__created_at__gte=period_start,
            dispensing__created_at__lt=period_end,
        ).aggregate(total=Sum('quantity'))
        dispensed = dispensed_qs['total'] or 0
 
        # b) Manual outflows ONLY — expired / damaged removals via Stock Management.
        #    'out' is intentionally excluded: dispensing already creates a
        #    StockMovement(type='out'), so counting it here would double the total.
        manual_out_qs = StockMovement.objects.filter(
            medicine=med,
            movement_type__in=['expired', 'damaged'],
            created_at__gte=period_start,
            created_at__lt=period_end,
        ).aggregate(total=Sum('quantity'))
        manual_out = abs(manual_out_qs['total'] or 0)
 
        consumed = dispensed + manual_out
 
        # ── Balance on Hand ──────────────────────────────────────────────────
        balance_on_hand = previous_balance + received - consumed
 
        # ── Unit of Measure ──────────────────────────────────────────────────
        uom = med.dosage_form  # tablet / capsule / vial / etc.
 
        rows.append({
            'item_code':        med.barcode or f"MED-{med.pk:04d}",
            'description':      f"{med.medicine_name} {med.strength}",
            'unit_of_measure':  uom,
            'previous_balance': previous_balance,
            'received':         received,
            'consumed':         consumed,
            'balance_on_hand':  balance_on_hand,
        })
 
    # ── 3. Compute totals ─────────────────────────────────────────────────────
    total_received = sum(r['received']         for r in rows)
    total_consumed = sum(r['consumed']         for r in rows)
    total_balance  = sum(r['balance_on_hand']  for r in rows)
 
    # ── 4. Build month selector options ──────────────────────────────────────
    month_choices    = [(i, calendar.month_name[i]) for i in range(1, 13)]
    year_choices     = list(range(today.year - 3, today.year + 2))
    last_day_of_month = calendar.monthrange(year, month)[1]
 
    # Use the formatted name function
    submitted_by = get_formatted_name(request.user)
    position = request.user.profile.position if hasattr(request.user, 'profile') else '—'

    context = {
        'rows':              rows,
        'selected_year':     year,
        'selected_month':    month,
        'month_name':        calendar.month_name[month],
        'month_choices':     month_choices,
        'year_choices':      year_choices,
        'period_start':      period_start,
        'period_end':        period_end,
        'today':             today,
        'total_received':    total_received,
        'total_consumed':    total_consumed,
        'total_balance':     total_balance,
        'last_day_of_month': last_day_of_month,
        'submitted_by':      submitted_by,
        'position':          position,
    }
    return render(request, 'consumption_report.html', context)



# ─────────────────────────────────────────────────────────────────────────────
# PDEA S2 REGISTER  (printable "PDEA S Register 2-14" ledger, one medicine)
# ─────────────────────────────────────────────────────────────────────────────

REGISTER_UNITS = {
    'injection': 'AMP.', 'tablet': 'TAB.', 'capsule': 'CAP.', 'syrup': 'BTL.',
    'suspension': 'BTL.', 'drops': 'BTL.', 'cream': 'TUBE', 'ointment': 'TUBE',
    'inhaler': 'PC.', 'patch': 'PC.', 'suppository': 'PC.', 'powder': 'SACHET',
}
REGISTER_MOVE_LABELS = {
    'in': 'RECEIVED', 'return': 'RETURNED', 'adjust': 'ADJUSTMENT',
    'audit': 'AUDIT ADJ.', 'out': 'STOCK OUT', 'expired': 'EXPIRED', 'damaged': 'DAMAGED',
}


def _formatted_user_name(user):
    first  = (user.first_name or '').upper()
    last   = (user.last_name or '').upper()
    middle = getattr(getattr(user, 'profile', None), 'middle_name', '') or ''
    mi     = f'{middle[0].upper()}.' if middle else ''
    return ' '.join(p for p in (first, mi, last) if p) or user.username.upper()


def _register_supplier_lines(supplier):
    if not supplier:
        return []
    lines = [supplier.name.upper()]
    if supplier.address:
        lines.append(' '.join(supplier.address.split()))
    if supplier.pdea_permit_no:
        exp = supplier.pdea_permit_expiry.strftime('%m/%d/%Y') if supplier.pdea_permit_expiry else '—'
        lines.append(f'PDEA Permit No. {supplier.pdea_permit_no} (exp. {exp})')
    if supplier.s4_license_no:
        exp = supplier.s4_license_expiry.strftime('%m/%d/%Y') if supplier.s4_license_expiry else '—'
        lines.append(f'S4 License No. {supplier.s4_license_no} (exp. {exp})')
    return lines

def _pdea_register_build(med, date_from, date_to, rows_per_page, start_page):
    """Builds the register (pages + totals) for ONE medicine."""
    unit     = REGISTER_UNITS.get(med.dosage_form, 'PC.')
    start_dt = timezone.make_aware(datetime.combine(date_from, datetime.min.time()))
    prev     = (StockMovement.objects.filter(medicine=med, created_at__lt=start_dt)
                .order_by('-created_at', '-id').first())
    opening  = prev.quantity_after if prev else 0

    events = []

    # ── OUT: dispensing records (patient / ward, requisition, practitioner) ──
    items = (DispensingItem.objects
             .filter(medicine=med,
                     dispensing__created_at__date__gte=date_from,
                     dispensing__created_at__date__lte=date_to)
             .select_related('dispensing', 'dispensing__doctor', 'dispensing__ward'))
    for it in items:
        d, doc = it.dispensing, it.dispensing.doctor
        if d.patient_type == 'in':
            who = f'{d.ward.name.upper()} USED' if d.ward else 'IN-PATIENT USED'
        else:
            name = (d.customer_name or '').strip().upper()
            who  = f'{name} - OPD' if d.patient_type == 'out' and name else name
        practitioner = ''
        if doc:
            practitioner = f'DR. {doc.get_full_name().upper()}'
            if doc.specialization:
                practitioner += f' - {doc.specialization.upper()}'
        events.append({
            'dt': d.created_at, 'kind': 'out', 'qty': it.quantity,
            'patient': who, 'req': d.requisition_no or d.prescription_no,
            's2': doc.s2_license_no if doc else '', 'practitioner': practitioner,
            'remarks': {'in': 'IN-PATIENT', 'out': 'OUT-PATIENT'}.get(d.patient_type, ''),
        })

    # ── Other stock changes (receipts, returns, expired, adjustments …) ─────
    moves = (StockMovement.objects
             .filter(medicine=med,
                     created_at__date__gte=date_from, created_at__date__lte=date_to)
             .exclude(movement_type='transfer')
             .select_related('supplier'))
    grouped = {}
    for m in moves:
        if m.movement_type == 'out' and m.reference_no.startswith('TXN-'):
            continue                      # already listed from the dispensing record
        key = (m.movement_type, m.reference_no, m.notes, m.supplier_id,
               m.created_at.replace(microsecond=0))
        g = grouped.setdefault(key, {'m': m, 'qty': 0})
        g['qty'] += m.quantity
    for g in grouped.values():
        m, qty = g['m'], g['qty']
        if qty == 0:
            continue
        label = REGISTER_MOVE_LABELS.get(m.movement_type, m.movement_type.upper())
        if qty > 0:
            lines = _register_supplier_lines(m.supplier) if m.movement_type == 'in' else []
            if not lines:
                lines = [label]
            if m.reference_no:
                lines.append(f'Ref: {m.reference_no}')
            if m.notes and m.movement_type != 'in':
                lines.append(m.notes)
            events.append({'dt': m.created_at, 'kind': 'in', 'qty': qty,
                           'info_lines': lines, 'remarks': label})
        else:
            events.append({
                'dt': m.created_at, 'kind': 'out', 'qty': abs(qty),
                'patient': ' — '.join(p for p in (label, m.dispensed_to, m.notes) if p),
                'req': m.prescription_no or m.reference_no,
                's2': '', 'practitioner': '', 'remarks': label,
            })

    # ── Running balance ──────────────────────────────────────────────────────
    events.sort(key=lambda e: e['dt'])
    bal = opening
    for e in events:
        bal += e['qty'] if e['kind'] == 'in' else -e['qty']
        e['balance'] = bal
        e['date']    = timezone.localtime(e['dt']).strftime('%m/%d/%Y')

    # ── Split into printed pages (each page carries its own balance forwarded)
    chunks = [events[i:i + rows_per_page] for i in range(0, len(events), rows_per_page)] or [[]]
    pages, carry = [], opening
    for n, chunk in enumerate(chunks):
        pages.append({
            'number': start_page + n, 'forwarded': carry, 'rows': chunk,
            'blank_rows': range(rows_per_page - len(chunk)),
        })
        if chunk:
            carry = chunk[-1]['balance']

    return {
        'med': med, 'unit': unit, 'pages': pages,
        'opening': opening, 'closing': bal,
        'total_in':  sum(e['qty'] for e in events if e['kind'] == 'in'),
        'total_out': sum(e['qty'] for e in events if e['kind'] == 'out'),
        'has_events': bool(events),
    }


def _pdea_register_context(request):
    """Filters + registers, shared by the page, the PDF and the Excel export."""
    today   = timezone.localdate()
    s2_meds = Medicine.objects.filter(is_active=True, is_s2_regulated=True).order_by('medicine_name')

    date_from = _parse_iso(request.GET.get('date_from'), today.replace(day=1))
    date_to   = _parse_iso(request.GET.get('date_to'), today)
    if date_to < date_from:
        date_from, date_to = date_to, date_from

    rows_options = (10, 15, 20, 25, 30)
    try:
        rows_per_page = int(request.GET.get('rows', 15))
    except (TypeError, ValueError):
        rows_per_page = 15
    if rows_per_page not in rows_options:
        rows_per_page = 15
    try:
        start_page = max(int(request.GET.get('start_page', 1)), 1)
    except (TypeError, ValueError):
        start_page = 1

    med_param = request.GET.get('medicine', '').strip()
    med_id    = _to_int(med_param)
    single    = s2_meds.filter(pk=med_id).first() if med_id else None

    if med_param == 'all':
        targets = list(s2_meds)
    elif single:
        targets = [single]
    else:
        targets = []          # nothing chosen yet -> empty page

    registers = []
    for m in targets:
        reg = _pdea_register_build(m, date_from, date_to, rows_per_page, start_page)
        if med_param == 'all' and not reg['has_events'] and reg['opening'] == 0:
            continue
        registers.append(reg)

    if med_param == 'all':
        selected_label = 'All S2 medicines (one register each)'
    elif single:
        selected_label = f'{single.medicine_name} {single.strength}'
    else:
        selected_label = ''

    return {
        's2_meds': s2_meds, 'registers': registers,
        'selected': single.pk if single else None,
        'date_from': date_from, 'date_to': date_to,
        'rows_per_page': rows_per_page, 'rows_options': rows_options,
        'start_page': start_page,
        'pharmacist': request.GET.get('pharmacist', '').strip() or _formatted_user_name(request.user),
        'med_param': med_param if (med_param == 'all' or single) else '',
        'selected_label': selected_label,
        'generated': bool(med_param == 'all' or single),
    }


@require('pdea_register')
def pdea_register_report(request):
    return render(request, 'pdea_register.html', _pdea_register_context(request))


def _pdea_register_filename(ctx, ext):
    regs = ctx['registers']
    tag  = re.sub(r'[^A-Za-z0-9]+', '_', regs[0]['med'].generic_name).strip('_') \
           if ctx['selected'] and regs else 'ALL'
    return f"PDEA_S2_Register_{tag}_{ctx['date_from']}_{ctx['date_to']}.{ext}"


# ── PDF ──────────────────────────────────────────────────────────────────────
@require('pdea_register')
def pdea_register_export_pdf(request):
    if not REPORTLAB_AVAILABLE:
        messages.error(request, 'reportlab is not installed. Run: pip install reportlab')
        return redirect('pdea_register')
    from reportlab.platypus import PageBreak

    ctx = _pdea_register_context(request)
    if not ctx['registers']:
        messages.error(request, 'No S2 register data to export for these filters.')
        return redirect('pdea_register')

    FOLIO   = (8.5 * inch, 13 * inch)
    page_sz = landscape(FOLIO)                       # 13 x 8.5 in
    margin  = 0.4 * inch
    avail_w = page_sz[0] - 2 * margin
    black   = colors.black

    buffer = io.BytesIO()
    doc = SimpleDocTemplate(buffer, pagesize=page_sz, leftMargin=margin, rightMargin=margin,
                            topMargin=margin, bottomMargin=margin,
                            title='PDEA S2 Register', author='MHARSMC ePIS')

    styles = getSampleStyleSheet()
    base   = ParagraphStyle('rb', parent=styles['Normal'], fontName='Helvetica', fontSize=7.5, leading=9)
    cell_c = ParagraphStyle('rc', parent=base, alignment=TA_CENTER)
    cell_b = ParagraphStyle('rcb', parent=base, alignment=TA_CENTER, fontName='Helvetica-Bold')
    head   = ParagraphStyle('rh', parent=base, alignment=TA_CENTER, fontName='Helvetica-Bold', fontSize=6.8, leading=8)
    grp    = ParagraphStyle('rg', parent=head, fontSize=10, leading=12)
    t_box  = ParagraphStyle('rt', parent=base, fontSize=9, leading=11)
    r9     = ParagraphStyle('r9', parent=base, fontSize=9, leading=11, alignment=TA_RIGHT)
    r9b    = ParagraphStyle('r9b', parent=r9, fontName='Helvetica-Bold')
    medst  = ParagraphStyle('rm', parent=base, fontName='Helvetica-Bold', fontSize=12, leading=14)
    c10b   = ParagraphStyle('c10', parent=base, fontName='Helvetica-Bold', fontSize=10, leading=12, alignment=TA_CENTER)
    c8     = ParagraphStyle('c8', parent=base, fontSize=8, leading=10, alignment=TA_CENTER)

    def P(text, style=cell_c):
        return Paragraph(_xml_escape(str(text)).replace('\n', '<br/>'), style)

    fr    = [6, 16, 6, 6, 14, 9, 10, 14, 6, 7, 6]       # column widths in %
    col_w = [avail_w * f / 100 for f in fr]
    head_labels = [
        'Date<br/>Received',
        'Supplier Information:<br/>Name, address, PDEA license no./ Local Permit No. and date issued',
        'Quantity<br/>Received',
        'Date<br/>Dispensed',
        'Patient Information: Name, age, ward<br/>or<br/>Ward Information: Name of Ward, Receiving nurse',
        'Rx/Order No.<br/>or<br/>Requisition No.',
        'S2<br/>license<br/>No.',
        'Practitioner Information:<br/>Name, Department',
        'Quantity<br/>Dispensed', 'Balance', 'Remarks',
    ]

    story, first = [], True
    for reg in ctx['registers']:
        med, unit = reg['med'], reg['unit']
        for pg in reg['pages']:
            if not first:
                story.append(PageBreak())
            first = False

            # ── top: title box + page no ──
            top = Table([[Paragraph('<b>PDEA S Register 2-14</b><br/>( Records required of a hospital pharmacy)', t_box),
                          Paragraph(f'Page No. {pg["number"]}', r9)]],
                        colWidths=[3.3 * inch, avail_w - 3.3 * inch])
            top.setStyle(TableStyle([('BOX', (0, 0), (0, 0), 0.8, black),
                                     ('VALIGN', (0, 0), (-1, -1), 'BOTTOM'),
                                     ('LEFTPADDING', (0, 0), (-1, -1), 4), ('RIGHTPADDING', (0, 0), (-1, -1), 4)]))
            story += [top, Spacer(1, 6)]

            # ── medicine + balance forwarded ──
            meta = Table([[Paragraph(_xml_escape(f'{med.generic_name.upper()} {med.strength} {unit.lower()}'), medst),
                           Paragraph('Initial Stock / Balance Forwarded', r9b),
                           P(f'{pg["forwarded"]:,} {unit}', cell_b)]],
                         colWidths=[avail_w - 4.9 * inch, 3.2 * inch, 1.7 * inch])
            meta.setStyle(TableStyle([('BOX', (2, 0), (2, 0), 0.8, black),
                                      ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
                                      ('TOPPADDING', (2, 0), (2, 0), 4), ('BOTTOMPADDING', (2, 0), (2, 0), 4)]))
            story += [meta, Spacer(1, 4)]

            # ── ledger table ──
            data = [[P('IN', grp), '', ''] + [P('OUT', grp)] + [''] * 7,
                    [Paragraph(h, head) for h in head_labels]]
            for rw in pg['rows']:
                if rw['kind'] == 'in':
                    data.append([P(rw['date']), P('\n'.join(rw['info_lines']), ParagraphStyle('l', parent=base)),
                                 P(f'{rw["qty"]:,} {unit}', cell_b), '', '', '', '', '', '',
                                 P(f'{rw["balance"]:,} {unit}', cell_b), P(rw['remarks'])])
                else:
                    data.append(['', '', '', P(rw['date']), P(rw['patient']), P(rw['req']), P(rw['s2']),
                                 P(rw['practitioner']), P(f'{rw["qty"]:,} {unit}', cell_b),
                                 P(f'{rw["balance"]:,} {unit}', cell_b), P(rw['remarks'])])
            blanks = len(pg['blank_rows'])
            for _ in range(blanks):
                data.append([''] * 11)

            heights = [None, None] + [None] * len(pg['rows']) + [0.2 * inch] * blanks
            tbl = Table(data, colWidths=col_w, rowHeights=heights, repeatRows=2)
            tbl.setStyle(TableStyle([
                ('GRID', (0, 0), (-1, -1), 0.6, black),
                ('SPAN', (0, 0), (2, 0)), ('SPAN', (3, 0), (10, 0)),
                ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
                ('TOPPADDING', (0, 0), (-1, -1), 2), ('BOTTOMPADDING', (0, 0), (-1, -1), 2),
                ('LEFTPADDING', (0, 0), (-1, -1), 3), ('RIGHTPADDING', (0, 0), (-1, -1), 3),
            ]))
            story.append(tbl)

            # ── signature ──
            story += [
                Spacer(1, 0.22 * inch),
                Paragraph(f'<b>{_xml_escape(ctx["pharmacist"])}</b>', c10b),
                HRFlowable(width='32%', thickness=0.8, color=black, hAlign='CENTER', spaceBefore=1, spaceAfter=2),
                Paragraph('SIGNATURE OVER PRINTED NAME OF AUTHORIZED PHARMACIST', c8),
            ]

    doc.build(story)
    resp = HttpResponse(buffer.getvalue(), content_type='application/pdf')
    resp['Content-Disposition'] = f'attachment; filename="{_pdea_register_filename(ctx, "pdf")}"'
    return resp


# ── Excel ────────────────────────────────────────────────────────────────────
def _xl_merge(ws, r1, c1, r2, c2, value='', font=None, align=None, border=None):
    if (r1, c1) != (r2, c2):
        ws.merge_cells(start_row=r1, start_column=c1, end_row=r2, end_column=c2)
    cell = ws.cell(r1, c1, value)
    if font:
        cell.font = font
    if align:
        cell.alignment = align
    if border:
        for rr in range(r1, r2 + 1):
            for cc in range(c1, c2 + 1):
                ws.cell(rr, cc).border = border
    return cell


def _xl_lines(text, width):
    if not text:
        return 1
    return sum(max(1, math.ceil(len(p) / max(width - 1, 1))) for p in str(text).split('\n'))


def _xl_sheet_title(med, used):
    base  = re.sub(r'[\[\]:*?/\\]', '', f'{med.generic_name} {med.strength}').strip()[:28] or 'Register'
    title, n = base, 2
    while title in used:
        title = f'{base[:25]}-{n}'
        n += 1
    used.add(title)
    return title


@require('pdea_register')
def pdea_register_export_excel(request):
    if not OPENPYXL_AVAILABLE:
        messages.error(request, 'openpyxl is not installed. Run: pip install openpyxl')
        return redirect('pdea_register')
    from openpyxl.worksheet.pagebreak import Break
    from openpyxl.worksheet.properties import PageSetupProperties

    ctx = _pdea_register_context(request)
    registers = ctx['registers']
    if not registers:
        messages.error(request, 'No S2 register data to export for these filters.')
        return redirect('pdea_register')

    thin   = Side(style='thin', color='000000')
    box    = Border(left=thin, right=thin, top=thin, bottom=thin)
    f_norm = Font(name='Arial', size=9)
    f_bold = Font(name='Arial', size=9, bold=True)
    f_head = Font(name='Arial', size=8, bold=True)
    f_med  = Font(name='Arial', size=13, bold=True)
    f_grp  = Font(name='Arial', size=11, bold=True)
    ctr    = Alignment(horizontal='center', vertical='center', wrap_text=True)
    lft    = Alignment(horizontal='left', vertical='center', wrap_text=True)
    rgt    = Alignment(horizontal='right', vertical='center')
    widths = [11, 38, 11, 11, 30, 18, 16, 32, 11, 11, 14]
    head_labels = [
        'Date\nReceived',
        'Supplier Information:\nName, address, PDEA license no./ Local Permit No. and date issued',
        'Quantity\nReceived', 'Date\nDispensed',
        'Patient Information: Name, age, ward\nor\nWard Information: Name of Ward, Receiving nurse',
        'Rx/Order No.\nor\nRequisition No.', 'S2\nlicense\nNo.',
        'Practitioner Information:\nName, Department',
        'Quantity\nDispensed', 'Balance', 'Remarks',
    ]

    wb = openpyxl.Workbook()
    wb.remove(wb.active)

    # ── Summary sheet (only when exporting several medicines) ────────────────
    if len(registers) > 1:
        ws = wb.create_sheet('Summary')
        ws.append(['MHARSMC — PDEA S2 Register Summary'])
        ws['A1'].font = Font(name='Arial', size=13, bold=True)
        ws.append([f'{ctx["date_from"]} to {ctx["date_to"]}'])
        ws.append([])
        ws.append(['Medicine', 'Balance Forwarded', 'Received', 'Dispensed / Removed', 'Closing Balance', 'Pages'])
        for c in ws[4]:
            c.font = Font(name='Arial', size=10, bold=True, color='FFFFFF')
            c.fill = PatternFill('solid', fgColor='00ADB5')
            c.alignment = ctr
        for reg in registers:
            ws.append([f'{reg["med"].generic_name} {reg["med"].strength}', reg['opening'],
                       reg['total_in'], reg['total_out'], reg['closing'], len(reg['pages'])])
        for i, w in enumerate([42, 18, 12, 20, 16, 8], 1):
            ws.column_dimensions[get_column_letter(i)].width = w

    # ── One worksheet per medicine ───────────────────────────────────────────
    used = {'Summary'}
    for reg in registers:
        med, unit = reg['med'], reg['unit']
        ws = wb.create_sheet(_xl_sheet_title(med, used))
        for i, w in enumerate(widths, 1):
            ws.column_dimensions[get_column_letter(i)].width = w
        ws.page_setup.orientation = 'landscape'
        ws.page_setup.paperSize   = 14            # Folio 8.5 x 13 in
        ws.page_setup.fitToWidth  = 1
        ws.page_setup.fitToHeight = 0
        ws.sheet_properties.pageSetUpPr = PageSetupProperties(fitToPage=True)
        ws.page_margins.left = ws.page_margins.right = 0.4
        ws.page_margins.top  = ws.page_margins.bottom = 0.4

        r = 1
        for pg in reg['pages']:
            # title box + page no
            _xl_merge(ws, r, 1, r + 1, 3,
                      'PDEA S Register 2-14\n( Records required of a hospital pharmacy)', f_bold, lft, box)
            _xl_merge(ws, r, 10, r, 11, f'Page No. {pg["number"]}', f_bold, rgt)
            ws.row_dimensions[r].height = 16
            ws.row_dimensions[r + 1].height = 16
            r += 3

            # medicine + balance forwarded
            _xl_merge(ws, r, 1, r, 5, f'{med.generic_name.upper()} {med.strength} {unit.lower()}', f_med, lft)
            _xl_merge(ws, r, 6, r, 9, 'Initial Stock / Balance Forwarded', f_bold, rgt)
            _xl_merge(ws, r, 10, r, 11, f'{pg["forwarded"]:,} {unit}', f_bold, ctr, box)
            ws.row_dimensions[r].height = 22
            r += 2

            # header rows
            _xl_merge(ws, r, 1, r, 3, 'IN', f_grp, ctr, box)
            _xl_merge(ws, r, 4, r, 11, 'OUT', f_grp, ctr, box)
            r += 1
            for i, label in enumerate(head_labels, 1):
                c = ws.cell(r, i, label)
                c.font, c.alignment, c.border = f_head, ctr, box
            ws.row_dimensions[r].height = 62
            r += 1

            # data rows
            for rw in pg['rows']:
                if rw['kind'] == 'in':
                    vals = [rw['date'], '\n'.join(rw['info_lines']), f'{rw["qty"]:,} {unit}',
                            '', '', '', '', '', '', f'{rw["balance"]:,} {unit}', rw['remarks']]
                else:
                    vals = ['', '', '', rw['date'], rw['patient'], rw['req'], rw['s2'], rw['practitioner'],
                            f'{rw["qty"]:,} {unit}', f'{rw["balance"]:,} {unit}', rw['remarks']]
                lines = 1
                for i, v in enumerate(vals, 1):
                    c = ws.cell(r, i, v)
                    c.font, c.border = f_norm, box
                    c.alignment = lft if i in (2,) else ctr
                    lines = max(lines, _xl_lines(v, widths[i - 1]))
                ws.row_dimensions[r].height = max(16, 12.5 * lines + 4)
                r += 1
            for _ in pg['blank_rows']:
                for i in range(1, 12):
                    ws.cell(r, i).border = box
                ws.row_dimensions[r].height = 17
                r += 1

            # signature
            r += 1
            _xl_merge(ws, r, 4, r, 8, ctx['pharmacist'], Font(name='Arial', size=10, bold=True), ctr,
                      Border(bottom=thin))
            r += 1
            _xl_merge(ws, r, 4, r, 8, 'SIGNATURE OVER PRINTED NAME OF AUTHORIZED PHARMACIST',
                      Font(name='Arial', size=8), ctr)
            ws.row_breaks.append(Break(id=r))          # next register page starts on a new sheet
            r += 2

    resp = HttpResponse(
        content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
    resp['Content-Disposition'] = f'attachment; filename="{_pdea_register_filename(ctx, "xlsx")}"'
    wb.save(resp)
    return resp


# ─────────────────────────────────────────────────────────────────────────────
# DISPENSING
# ─────────────────────────────────────────────────────────────────────────────

@require('dispensing')
def dispensing_view(request):
    today = timezone.now().date()

    today_txns    = Dispensing.objects.filter(created_at__date=today)
    txn_today     = today_txns.count()
    units_today   = DispensingItem.objects.filter(
                        dispensing__created_at__date=today).aggregate(
                        t=Sum('quantity'))['t'] or 0
    revenue_today = today_txns.aggregate(t=Sum('total_amount'))['t'] or 0
    month_start   = today.replace(day=1)
    txn_month     = Dispensing.objects.filter(created_at__date__gte=month_start).count()

    per_page    = 10
    txn_qs      = Dispensing.objects.select_related('pharmacist').prefetch_related('items').all()
    paginator   = Paginator(txn_qs, per_page)
    page_number = request.GET.get('page', 1)
    try:
        page_obj = paginator.page(page_number)
    except (EmptyPage, PageNotAnInteger):
        page_obj = paginator.page(1)

    context = {
        'txn_today':     txn_today,
        'units_today':   units_today,
        'revenue_today': revenue_today,
        'txn_month':     txn_month,
        'transactions':  page_obj.object_list,
        'page_obj':      page_obj,
        'paginator':     paginator,
        'wards':         Ward.objects.filter(is_active=True),
    }
    return render(request, 'dispensing.html', context)


@require('dispensing', 'edit', ajax=True)
@transaction.atomic
def dispensing_create(request):
    if request.method != 'POST':
        return JsonResponse({'error': 'Invalid method.'}, status=405)

    try:
        data            = json.loads(request.body)
        items_data      = data.get('items', [])
        customer_name   = data.get('customer_name', '').strip()
        prescription_no = data.get('prescription_no', '').strip()
        notes           = data.get('notes', '').strip()
        discount_amount = float(data.get('discount_amount', 0))
        doctor_pk       = data.get('doctor_pk', '')

        # ── iHOMIS patient link (optional) ────────────────────────────────────
        # Sent from the frontend when the pharmacist selects a patient from iHOMIS.
        # ihomis_hpatkey is the hperson.hpatkey (integer PK in iHOMIS).
        # ihomis_hpercode is the hospital number — cached locally so we don't
        # need a live iHOMIS query just to display it.
        # Safe int conversion — prevents string hospital numbers from slipping in
        ihomis_hpatkey  = str(data.get('ihomis_hpatkey',  '') or '').strip() or None
        ihomis_hpercode = str(data.get('ihomis_hpercode', '') or '').strip()

        # Default customer name to Walk-in if nothing was provided
        if not customer_name:
            customer_name = 'Walk-in'

        # Safely handle doctor_pk
        if doctor_pk is not None:
            doctor_pk = str(doctor_pk).strip()
        else:
            doctor_pk = ''

        if not items_data:
            return JsonResponse({'error': 'Cart is empty.'}, status=400)

        # ── Validate all items before any DB writes ───────────────────────────
        validated   = []
        has_s2_item = False

        for item in items_data:
            pk  = item.get('medicine_pk')
            qty = int(item.get('quantity', 0))
            if qty <= 0:
                return JsonResponse({'error': 'Quantity must be > 0.'}, status=400)
            medicine = get_object_or_404(Medicine, pk=pk, is_active=True)
            if qty > medicine.stock_quantity:
                return JsonResponse({
                    'error': f'Insufficient stock for "{medicine.medicine_name}". '
                             f'Available: {medicine.stock_quantity}.'
                }, status=400)
            if medicine.is_s2_regulated:
                has_s2_item = True
            validated.append({
                'medicine':   medicine,
                'qty':        qty,
                'unit_price': float(item.get('unit_price', medicine.selling_price)),
            })

        # ── S2 hard block ─────────────────────────────────────────────────────
        doctor = None
        if has_s2_item:
            if not doctor_pk:
                return JsonResponse({
                    'error': 'A prescribing doctor with a valid S2 license is required '
                             'for S2-regulated medicines.'
                }, status=400)
            try:
                try:
                    doctor_pk_int = int(doctor_pk)
                    doctor = Doctor.objects.get(pk=doctor_pk_int, is_active=True)
                except (ValueError, TypeError):
                    doctor = Doctor.objects.get(pk=doctor_pk, is_active=True)
            except Doctor.DoesNotExist:
                return JsonResponse({
                    'error': 'Selected doctor not found or is inactive.'
                }, status=400)

            if not doctor.s2_is_valid_for_dispensing:
                status = doctor.s2_license_status
                if status == 'none':
                    reason = f'Dr. {doctor.get_full_name()} has no S2 license on record.'
                elif status == 'expired':
                    reason = (f'Dr. {doctor.get_full_name()}\'s S2 license expired on '
                              f'{doctor.s2_expiry_date}.')
                else:
                    reason = f'Dr. {doctor.get_full_name()}\'s S2 license is not valid.'
                return JsonResponse({'error': reason}, status=400)
        # ─────────────────────────────────────────────────────────────────────

        # ── S2: patient type + requisition number ─────────────────────────────
        patient_type = ''
        ward = None
        requisition_no = ''
        requisition_seq = requisition_year = requisition_half = None
        if has_s2_item:
            patient_type = str(data.get('patient_type', '') or '').strip()
            if patient_type not in ('in', 'out'):
                return JsonResponse({'error': 'Select In Patient or Out Patient for S2 medicines.'}, status=400)
            if patient_type == 'in':
                try:
                    # row lock so two pharmacists can't get the same number
                    ward = Ward.objects.select_for_update().get(
                        pk=int(data.get('ward_pk') or 0), is_active=True)
                except (Ward.DoesNotExist, ValueError, TypeError):
                    return JsonResponse({'error': 'Select a valid ward for In Patient.'}, status=400)
                today_local      = timezone.localdate()
                requisition_year = today_local.year
                requisition_half = 1 if today_local.month <= 6 else 2
                requisition_seq, requisition_no = _next_requisition(
                    ward, requisition_year, requisition_half)
            else:
                requisition_no = str(data.get('requisition_no', '') or '').strip()[:100]
                if not requisition_no:
                    return JsonResponse({'error': 'Enter the Requisition No. for Out Patient.'}, status=400)
        

        subtotal = sum(v['unit_price'] * v['qty'] for v in validated)
        discount = min(discount_amount, subtotal)
        total    = subtotal - discount

        # ── Create transaction ────────────────────────────────────────────────
        txn = Dispensing.objects.create(
            customer_name   = customer_name or 'Walk-in',
            prescription_no = prescription_no,
            notes           = notes,
            subtotal_amount = subtotal,
            discount_amount = discount,
            total_amount    = total,
            pharmacist      = request.user,
            doctor          = doctor,
            # iHOMIS link — stored as plain integers/strings, no cross-DB FK
            ihomis_hpatkey  = ihomis_hpatkey or '',
            ihomis_hpercode = ihomis_hpercode or '',
            patient_type    = patient_type,
            ward            = ward,
            requisition_no  = requisition_no,
            requisition_seq = requisition_seq,
            requisition_year= requisition_year,
            requisition_half= requisition_half,
        )

        # ── Create line items & deduct stock (FEFO) ───────────────────────────
        receipt_items = []
        for v in validated:
            medicine = v['medicine']
            qty      = v['qty']
            price    = v['unit_price']

            DispensingItem.objects.create(
                dispensing = txn,
                medicine   = medicine,
                quantity   = qty,
                unit_price = price,
            )

            _deduct_fefo(
                medicine, qty, request.user,
                movement_type   = 'out',
                dispensed_to    = customer_name,
                prescription_no = prescription_no,
                reference_no    = f'TXN-{txn.pk}',
                notes           = (
                    f'Dispensing #{txn.pk}'
                    + (f' — S2 Rx by Dr. {doctor.get_full_name()}' if doctor else '')
                    + (f' — {notes}' if notes else '')
                ),
            )

            receipt_items.append({
                'name':       medicine.medicine_name,
                'strength':   medicine.strength,
                'qty':        qty,
                'unit_price': price,
                'subtotal':   price * qty,
                'is_s2':      medicine.is_s2_regulated,
            })

        # ── Build doctor info for receipt ─────────────────────────────────────
        doctor_info = None
        if doctor:
            doctor_info = {
                'name':           f'Dr. {doctor.get_full_name()}',
                'specialization': doctor.specialization,
                'prc_license_no': doctor.prc_license_no,
                's2_license_no':  doctor.s2_license_no,
            }

        return JsonResponse({
            'success': True,
            'receipt': {
                'pk':              txn.pk,
                'date':            txn.created_at.strftime('%B %d, %Y %I:%M %p'),
                'pharmacist':      request.user.get_full_name() or request.user.username,
                'customer_name':   customer_name,
                'prescription_no': prescription_no,
                'items':           receipt_items,
                'subtotal':        subtotal,
                'discount':        discount,
                'total':           total,
                'doctor':          doctor_info,
                'patient_type': {'in': 'In Patient', 'out': 'Out Patient'}.get(patient_type, ''),
                'ward': ward.name if ward else '',
                'requisition_no': requisition_no,
                # Echo back the linked patient for frontend confirmation
                'ihomis_hpatkey':  ihomis_hpatkey,
                'ihomis_hpercode': ihomis_hpercode,
            }
        })

    except ValueError as e:
        return JsonResponse({'error': str(e)}, status=400)
    except Exception as e:
        return JsonResponse({'error': str(e)}, status=400)

@require(('dispensing', 'dispensing_card'), ajax=True)
def dispensing_receipt(request, pk):
    txn = get_object_or_404(
        Dispensing.objects.select_related('pharmacist', 'ward').prefetch_related('items__medicine'),
        pk=pk
    )
    items = [
        {
            'name':       item.medicine.medicine_name,
            'strength':   item.medicine.strength,
            'qty':        item.quantity,
            'unit_price': float(item.unit_price),
            'subtotal':   float(item.subtotal),
        }
        for item in txn.items.all()
    ]

    doctor_info = None
    if txn.doctor:
        d = txn.doctor
        doctor_info = {
            'name':           f'Dr. {d.get_full_name()}',
            'specialization': d.specialization,
            'prc_license_no': d.prc_license_no,
            's2_license_no':  d.s2_license_no,
        }

    return JsonResponse({
        'pk':              txn.pk,
        'date':            txn.created_at.strftime('%B %d, %Y %I:%M %p'),
        'pharmacist':      txn.pharmacist.get_full_name() if txn.pharmacist else '—',
        'customer_name':   txn.customer_name,
        'prescription_no': txn.prescription_no,
        'items':           items,
        'subtotal':        float(txn.subtotal_amount),
        'discount':        float(txn.discount_amount),
        'total':           float(txn.total_amount),
        'doctor':          doctor_info,
        'patient_type': txn.get_patient_type_display() if txn.patient_type else '',
        'ward': txn.ward.name if txn.ward else '',
        'requisition_no': txn.requisition_no,
    })



# views_dispensing_card.py
# Add these views to your existing views.py (or import from here).
# They power: patient search, dispensing card page, and printable card.
# ─────────────────────────────────────────────────────────────────────────────
# AJAX: Search iHOMIS patients
# URL:  /dispensing/patient-search/?q=<term>
# ─────────────────────────────────────────────────────────────────────────────
@require(('dispensing', 'dispensing_card'), ajax=True)
@require_GET
def patient_search_ajax(request):
    q = request.GET.get('q', '').strip()

    if len(q) < 2:
        return JsonResponse({'results': []})

    tokens = [t for t in q.replace(',', ' ').split() if t]
    if not tokens:
        return JsonResponse({'results': []})

    try:
        qs = HPerson.objects.using('ihomis_plus').all()

        for token in tokens:
            qs = qs.filter(
                Q(hpercode__icontains=token) |
                Q(hpatcode__icontains=token) |
                Q(patlast__icontains=token)  |
                Q(patfirst__icontains=token) |
                Q(patmedno__icontains=token)
            )

        # Rank: last name starts with it > last name contains it >
        # first name starts with it > everything else
        first = tokens[0]
        qs = qs.annotate(
            rank=Case(
                When(patlast__istartswith=first, then=Value(0)),
                When(patlast__icontains=first,   then=Value(1)),
                When(patfirst__istartswith=first, then=Value(2)),
                default=Value(3),
                output_field=IntegerField(),
            )
        ).order_by('rank', 'patlast', 'patfirst')[:40]

        results = []
        for p in qs:
            results.append({
                'hpatkey':         str(p.hpatkey),
                'hospital_number': p.hospital_number,
                'full_name':       p.full_name,
                'full_name_formal':p.full_name_formal,
                'birthdate':       p.patbdate.strftime('%B %d, %Y') if p.patbdate else '—',
                'sex':             p.sex_display,
                'address':         p.address,
                'phicnum':         p.phicnum or '—',
                'patmedno':        p.patmedno or '—',
            })
    except Exception as e:
        return JsonResponse({'results': [], 'error': str(e)})

    return JsonResponse({'results': results})


# ─────────────────────────────────────────────────────────────────────────────
# AJAX: Fetch one iHOMIS patient by hpatkey
# URL:  /dispensing/patient/<hpatkey>/info/
# ─────────────────────────────────────────────────────────────────────────────
@require(('dispensing', 'dispensing_card'), ajax=True)
@require_GET
def patient_info_ajax(request, hpatkey):
    try:
        p = HPerson.objects.using('ihomis_plus').get(pk=hpatkey)
        return JsonResponse({
            'hpatkey':         p.hpatkey,
            'hospital_number': p.hospital_number,
            'full_name':       p.full_name,
            'full_name_formal':p.full_name_formal,
            'birthdate':       p.patbdate.strftime('%B %d, %Y') if p.patbdate else '—',
            'birthdate_iso':   p.patbdate.isoformat() if p.patbdate else '',
            'sex':             p.sex_display,
            'address':         p.address,
            'phicnum':         p.phicnum or '—',
            'patmedno':        p.patmedno or '—',
            'civil_status':    p.patcstat or '—',
            'contact':         p.pattelno or '—',
        })
    except HPerson.DoesNotExist:
        return JsonResponse({'error': 'Patient not found'}, status=404)
    except Exception as e:
        return JsonResponse({'error': str(e)}, status=500)


# ─────────────────────────────────────────────────────────────────────────────
# PAGE: Dispensing Card list (search & select patient)
# URL:  /dispensing/card/
# ─────────────────────────────────────────────────────────────────────────────
@require('dispensing_card')
def dispensing_card_list(request):
    """
    Landing page for the Dispensing Card module.
    Shows a patient search box. On selection, loads the card via AJAX or
    redirects to the card detail page.
    """
    return render(request, 'dispensing_card.html', {
        'page': 'dispensing_card',
    })


# ─────────────────────────────────────────────────────────────────────────────
# PAGE/AJAX: Dispensing Card for one patient
# URL:  /dispensing/card/<hpatkey>/
# ─────────────────────────────────────────────────────────────────────────────
@require('dispensing_card')
def dispensing_card_detail(request, hpatkey):
    hpatkey_str = str(hpatkey).strip()
 
    # Fetch patient from iHOMIS
    try:
        patient = HPerson.objects.using('ihomis_plus').get(pk=hpatkey_str)
    except HPerson.DoesNotExist:
        print(f"[DEBUG] Patient not found: {hpatkey_str}")
        if request.headers.get('x-requested-with') == 'XMLHttpRequest':
            return JsonResponse({'error': f'Patient not found: {hpatkey_str}'}, status=404)
        return render(request, 'dispensing_card.html', {
            'error': f'Patient not found in iHOMIS.',
            'page':  'dispensing_card',
        })
    except Exception as e:
        if request.headers.get('x-requested-with') == 'XMLHttpRequest':
            return JsonResponse({'error': f'iHOMIS unavailable: {e}'}, status=503)
        return render(request, 'dispensing_card.html', {
            'error': f'Cannot reach iHOMIS database: {e}',
            'page':  'dispensing_card',
        })
 
    # Fetch dispensing records — try both the hpatkey and the hospital number
    # in case some records were saved with hpercode instead of hpatkey
    transactions_qs = (
        Dispensing.objects
        .filter(ihomis_hpatkey=hpatkey_str)
        .prefetch_related('items__medicine', 'doctor', 'pharmacist')
        .order_by('-created_at')
    )
 
    # Fallback: also check by hospital number (hpercode) in case that was stored
    if not transactions_qs.exists():
        hpercode = patient.hospital_number  # uses the property
        print(f"[DEBUG] No transactions found with hpatkey, trying hpercode {repr(hpercode)}")
        transactions_qs = (
            Dispensing.objects
            .filter(ihomis_hpercode=hpercode)
            .prefetch_related('items__medicine', 'doctor', 'pharmacist')
            .order_by('-created_at')
        )
 
    stats = transactions_qs.aggregate(
        total_transactions=Count('id'),
        total_amount=Sum('total_amount'),
        total_units=Sum('items__quantity'),
    )
 
    txn_list = []
    for txn in transactions_qs:
        items = []
        for item in txn.items.all():
            items.append({
                'medicine_name': item.medicine.medicine_name,
                'generic_name':  item.medicine.generic_name,
                'strength':      item.medicine.strength,
                'dosage_form':   item.medicine.get_dosage_form_display(),
                'quantity':      item.quantity,
                'unit_price':    float(item.unit_price),
                'subtotal':      float(item.subtotal),
                'is_s2':         item.medicine.is_s2_regulated,
            })
        txn_list.append({
            'pk':              txn.pk,
            'date':            txn.created_at.strftime('%B %d, %Y'),
            'time':            txn.created_at.strftime('%I:%M %p'),
            'datetime_iso':    txn.created_at.isoformat(),
            'prescription_no': txn.prescription_no or '—',
            'notes':           txn.notes or '',
            'subtotal':        float(txn.subtotal_amount),
            'discount':        float(txn.discount_amount),
            'total':           float(txn.total_amount),
            'pharmacist':      (txn.pharmacist.get_full_name() or txn.pharmacist.username)
                               if txn.pharmacist else '—',
            'doctor':          f'Dr. {txn.doctor.get_full_name()}' if txn.doctor else '—',
            'doctor_s2':       txn.doctor.s2_license_no if txn.doctor else '',
            'items':           items,
        })
 
    if request.headers.get('x-requested-with') == 'XMLHttpRequest':
        return JsonResponse({
            'patient': {
                'hpatkey':          patient.hpatkey,
                'hospital_number':  patient.hospital_number,
                'full_name':        patient.full_name,
                'full_name_formal': patient.full_name_formal,
                'birthdate':        patient.patbdate.strftime('%B %d, %Y') if patient.patbdate else '—',
                'sex':              patient.sex_display,
                'address':          patient.address,
                'phicnum':          patient.phicnum or '—',
                'patmedno':         patient.patmedno or '—',
            },
            'stats': {
                'total_transactions': stats['total_transactions'] or 0,
                'total_amount':       float(stats['total_amount'] or 0),
                'total_units':        stats['total_units'] or 0,
            },
            'transactions': txn_list,
        })
 
    return render(request, 'dispensing_card.html', {
        'page':         'dispensing_card',
        'patient':      patient,
        'transactions': txn_list,
        'stats':        stats,
        'hpatkey':      hpatkey_str,
    })

# ─────────────────────────────────────────────────────────────────────────────
# AJAX: Link an iHOMIS patient to a dispensing transaction
# URL:  /dispensing/<pk>/link-patient/   POST
# Called from the dispensing page when pharmacist selects a patient
# ─────────────────────────────────────────────────────────────────────────────
@require('dispensing', 'edit', ajax=True)
def link_patient_to_dispensing(request, pk):
    if request.method != 'POST':
        return JsonResponse({'error': 'POST required'}, status=405)

    txn = get_object_or_404(Dispensing, pk=pk)
    try:
        body     = json.loads(request.body)
        hpatkey  = body.get('hpatkey')
        hpercode = body.get('hpercode', '')

        if not hpatkey:
            return JsonResponse({'error': 'hpatkey required'}, status=400)

        txn.ihomis_hpatkey  = hpatkey
        txn.ihomis_hpercode = hpercode
        # Also update customer_name from iHOMIS for legacy display
        if body.get('full_name'):
            txn.customer_name = body['full_name']
        txn.save(update_fields=['ihomis_hpatkey', 'ihomis_hpercode', 'customer_name'])

        return JsonResponse({'success': True})
    except Exception as e:
        return JsonResponse({'error': str(e)}, status=500)




# ─────────────────────────────────────────────────────────────────────────────
# CATEGORY — AJAX search + create
# ─────────────────────────────────────────────────────────────────────────────

@require(('inventory', 'stock_management'), ajax=True)
def category_search_ajax(request):
    q  = request.GET.get('q', '').strip()
    qs = MedicineCategory.objects.all()
    if q:
        qs = qs.filter(name__icontains=q)
    qs = qs.order_by('name')[:20]
    return JsonResponse({'results': [{'pk': c.pk, 'name': c.name} for c in qs]})


@require('inventory', 'edit', ajax=True)
def category_create_ajax(request):
    if request.method != 'POST':
        return JsonResponse({'error': 'Invalid method.'}, status=405)
    try:
        data = json.loads(request.body)
        name = data.get('name', '').strip()
    except (json.JSONDecodeError, AttributeError):
        name = request.POST.get('name', '').strip()
    if not name:
        return JsonResponse({'error': 'Category name cannot be empty.'}, status=400)
    if MedicineCategory.objects.filter(name__iexact=name).exists():
        existing = MedicineCategory.objects.get(name__iexact=name)
        return JsonResponse({'pk': existing.pk, 'name': existing.name, 'existed': True})
    category = MedicineCategory.objects.create(name=name)
    return JsonResponse({'pk': category.pk, 'name': category.name, 'existed': False})


# ─────────────────────────────────────────────────────────────────────────────
# BATCH MANAGEMENT — AJAX edit + soft-delete
# ─────────────────────────────────────────────────────────────────────────────

@require('stock_management', 'edit', ajax=True)
def batch_edit_view(request, pk):
    """
    AJAX POST — edit a MedicineStock batch's correctable fields:
    batch_number, expiry_date, purchase_price, supplier.
    Quantity is NOT editable here — use Stock In / Adjust for that.
    """
    if request.method != 'POST':
        return JsonResponse({'error': 'Invalid method.'}, status=405)
    try:
        batch = get_object_or_404(MedicineStock, pk=pk)
        data  = json.loads(request.body)

        batch.batch_number   = data.get('batch_number', batch.batch_number)
        batch.purchase_price = data.get('purchase_price', batch.purchase_price)
        expiry = data.get('expiry_date', '')
        batch.expiry_date = expiry if expiry else None
        supplier_id = data.get('supplier_id', '')
        batch.supplier_id = supplier_id if supplier_id else None
        batch.save()

        return JsonResponse({
            'success':        True,
            'batch_number':   batch.batch_number,
            'expiry_date':    str(batch.expiry_date) if batch.expiry_date else '',
            'purchase_price': str(batch.purchase_price),
            'supplier_id':    batch.supplier_id or '',
            'message':        f'Batch "{batch.batch_number or pk}" updated successfully.',
        })
    except Exception as e:
        return JsonResponse({'error': str(e)}, status=400)


@require('stock_management', 'edit', ajax=True)
def batch_delete_view(request, pk):
    """
    AJAX POST — soft-delete a MedicineStock batch by zeroing its quantity.
    The batch row is kept for audit trail (StockMovement FK references it).
    Creates a StockMovement log entry to record the removal.
    """
    if request.method != 'POST':
        return JsonResponse({'error': 'Invalid method.'}, status=405)
    try:
        batch    = get_object_or_404(MedicineStock, pk=pk)
        medicine = batch.medicine
        reason   = json.loads(request.body).get('reason', '').strip() if request.body else ''

        if batch.quantity == 0:
            return JsonResponse({'error': 'Batch is already empty.'}, status=400)

        old_total = medicine.stock_quantity
        removed   = batch.quantity
        batch.quantity = 0
        batch.save()
        new_total = medicine.stock_quantity   # recomputed via property

        StockMovement.objects.create(
            medicine        = medicine,
            batch           = batch,
            movement_type   = 'adjust',
            quantity        = -removed,
            quantity_before = old_total,
            quantity_after  = new_total,
            batch_number    = batch.batch_number,
            expiry_date     = batch.expiry_date,
            notes           = reason or f'Batch removed via Batch Management (Batch: {batch.batch_number or pk})',
            performed_by    = request.user,
        )

        return JsonResponse({
            'success':        True,
            'removed':        removed,
            'new_total_stock':new_total,
            'message':        f'Batch "{batch.batch_number or pk}" removed. {removed} units deducted.',
        })
    except Exception as e:
        return JsonResponse({'error': str(e)}, status=400)
    


# ─────────────────────────────────────────────────────────────────────────────
# ACCESS MANAGEMENT
# ─────────────────────────────────────────────────────────────────────────────

@require(level='admin')
def access_management_view(request):
    """
    Admin-only page. Shows nothing to configure at the role level —
    individual user permissions are managed inside User Management.
    This view exists as a landing page and overview.
    """
    users_with_perms = User.objects.select_related(
        'profile', 'permissions'
    ).filter(is_active=True).order_by('first_name', 'last_name')

    context = {
        'users_with_perms': users_with_perms,
        'module_choices':   UserPermission.MODULE_CHOICES,
    }
    return render(request, 'access_management.html', context)



# ─────────────────────────────────────────────────────────────────────────────
# STORAGE LOCATIONS (floor plan)
# ─────────────────────────────────────────────────────────────────────────────

STORAGE_ORIENTATIONS = {
    'landscape': (860, 540),
    'portrait':  (580, 700),
    'square':    (700, 700),
}


def get_storage_areas():
    """Areas with a `.choices` list of storable locations (Inventory dropdown)."""
    return StorageArea.objects.prefetch_related(
        Prefetch(
            'locations',
            queryset=StorageLocation.objects.filter(kind='shelf').order_by('code'),
            to_attr='choices',
        )
    )


def _storage_url(area_id=None, **extra):
    params = {}
    if area_id:
        params['area'] = area_id
    params.update(extra)
    url = reverse('storage_locations')
    return url + ('?' + urlencode(params) if params else '')


def _to_int(value):
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


EXPIRY_WINDOW_DAYS = 90


def _live_meds_qs():
    """Active medicines with their in-stock batches preloaded (one extra query)."""
    return Medicine.objects.filter(is_active=True).prefetch_related(
        Prefetch('batches', queryset=MedicineStock.objects.filter(quantity__gt=0),
                 to_attr='live_batches')
    )


def _med_split(m):
    """{location_id: {'qty': n, 'expiry': earliest_date|None}} for one medicine.
    A batch with no location counts as the medicine's main location."""
    split = {}
    if m.location_id:
        split[m.location_id] = {'qty': 0, 'expiry': None}
    for b in m.live_batches:
        lid = b.location_id or m.location_id
        if not lid:
            continue
        e = split.setdefault(lid, {'qty': 0, 'expiry': None})
        e['qty'] += b.quantity
        if b.expiry_date and (e['expiry'] is None or b.expiry_date < e['expiry']):
            e['expiry'] = b.expiry_date
    return split


def _placements():
    """{location_id: {medicine_id: {'med', 'qty', 'expiry'}}}"""
    out = {}
    for m in _live_meds_qs():
        m.total_qty = sum(b.quantity for b in m.live_batches)
        for lid, e in _med_split(m).items():
            out.setdefault(lid, {})[m.pk] = {'med': m, 'qty': e['qty'], 'expiry': e['expiry']}
    return out


def _stock_state(m):
    """Status is based on the medicine's TOTAL stock across all locations."""
    qty = m.total_qty or 0
    if qty == 0:
        return 'out'
    return 'low' if qty <= m.reorder_level else 'ok'


def _loc_has_stock(loc):
    if loc.medicines.filter(is_active=True).exists():
        return True
    return MedicineStock.objects.filter(
        medicine__is_active=True, quantity__gt=0, location=loc
    ).exists()


@require('storage_locations')
def storage_locations(request):
    soon       = timezone.now().date() + timedelta(days=EXPIRY_WINDOW_DAYS)
    placements = _placements()
    areas = list(
        StorageArea.objects.prefetch_related(
            Prefetch('locations', queryset=StorageLocation.objects.order_by('code'))
        )
    )

    plan = []
    shelves = used = total_meds = 0
    for a in areas:
        locs = []
        for l in a.locations.all():
            pl       = placements.get(l.pk, {}) if l.kind == 'shelf' else {}
            alerts   = sum(1 for e in pl.values() if _stock_state(e['med']) != 'ok')
            expiring = sum(1 for e in pl.values() if e['expiry'] and e['expiry'] <= soon)
            if l.kind == 'shelf':
                shelves += 1
                total_meds += len(pl)
                if pl:
                    used += 1
            locs.append({
                'id': l.pk, 'code': l.code, 'name': l.name, 'kind': l.kind,
                'x': l.x, 'y': l.y, 'w': l.w, 'h': l.h,
                'notes': l.notes, 'count': len(pl), 'alert': alerts, 'expiring': expiring,
            })
        plan.append({
            'id': a.pk, 'name': a.name, 'description': a.description,
            'canvas_w': a.canvas_w, 'canvas_h': a.canvas_h, 'locations': locs,
        })

    can_edit  = can(request.user, 'storage_locations', 'edit')
    can_admin = is_admin(request.user)

    return render(request, 'floor_plan.html', {
        'plan_data': plan,
        'initial': {
            'area':  _to_int(request.GET.get('area')),
            'focus': _to_int(request.GET.get('focus')),
            'open':  _to_int(request.GET.get('open')),
            'edit':  bool(can_edit and request.GET.get('edit')),
        },
        'area_count':  len(areas),
        'shelf_count': shelves,
        'used_count':  used,
        'med_count':   total_meds,
        'kind_choices': StorageLocation.KIND_CHOICES,
        'can_manage': can_edit,
        'can_admin':  can_admin,
    })


# ── JSON: medicines inside one shelf (quantity = amount stored HERE) ─────────
@require(('storage_locations', 'inventory'), ajax=True)
def storage_location_medicines(request, pk):
    loc   = get_object_or_404(StorageLocation.objects.select_related('area'), pk=pk)
    today = timezone.now().date()
    soon  = today + timedelta(days=EXPIRY_WINDOW_DAYS)
    pl    = _placements().get(loc.pk, {})

    rows = []
    for e in sorted(pl.values(), key=lambda e: e['med'].medicine_name.lower()):
        m, exp = e['med'], e['expiry']
        if not exp:
            exp_state = ''
        elif exp < today:
            exp_state = 'expired'
        elif exp <= soon:
            exp_state = 'soon'
        else:
            exp_state = 'ok'
        rows.append({
            'id': m.pk, 'name': m.medicine_name, 'generic': m.generic_name,
            'brand': m.brand_name, 'strength': m.strength,
            'form': m.get_dosage_form_display(),
            'qty': e['qty'], 'total': m.total_qty, 'status': _stock_state(m),
            'expiry': exp.strftime('%b %d, %Y') if exp else '', 'expiry_state': exp_state,
            's2': m.is_s2_regulated,
        })
    return JsonResponse({
        'id': loc.pk, 'code': loc.code, 'name': loc.name, 'area': loc.area.name,
        'kind': loc.kind, 'notes': loc.notes, 'medicines': rows,
    })


# ── JSON: "where is this medicine?" (one result per location holding it) ─────
@require(('storage_locations', 'inventory'), ajax=True)
def storage_find_medicine(request):
    q = request.GET.get('q', '').strip()
    if len(q) < 2:
        return JsonResponse({'results': []})
    meds = list(
        _live_meds_qs()
        .filter(Q(medicine_name__icontains=q) | Q(generic_name__icontains=q) |
                Q(brand_name__icontains=q) | Q(barcode__icontains=q))
        .order_by('medicine_name')[:15]
    )
    splits  = {m.pk: _med_split(m) for m in meds}
    loc_ids = {lid for s in splits.values() for lid in s}
    locs    = StorageLocation.objects.select_related('area').in_bulk(loc_ids)
    results = []
    for m in meds:
        for lid, e in splits[m.pk].items():
            loc = locs.get(lid)
            if not loc:
                continue
            results.append({
                'name': m.medicine_name, 'strength': m.strength, 'qty': e['qty'],
                'loc_id': loc.pk, 'code': loc.code,
                'area_id': loc.area_id, 'area': loc.area.name,
            })
    return JsonResponse({'results': results})


# ── Transfer stock between locations (total never changes) ───────────────────
@require('storage_locations', 'edit', ajax=True)
@require_POST
def storage_location_transfer(request):
    try:
        data   = json.loads(request.body or b'{}')
        med_id = int(data['medicine_id'])
        src_id = int(data['from_id'])
        dst_id = int(data['to_id'])
        qty    = int(data['quantity'])
    except (ValueError, KeyError, TypeError, AttributeError):
        return JsonResponse({'ok': False, 'error': 'Invalid data.'}, status=400)
    notes = str(data.get('notes', '') or '').strip()[:300]

    if qty <= 0:
        return JsonResponse({'ok': False, 'error': 'Quantity must be greater than zero.'}, status=400)
    if src_id == dst_id:
        return JsonResponse({'ok': False, 'error': 'Choose a different destination.'}, status=400)

    with transaction.atomic():
        med = get_object_or_404(Medicine.objects.select_for_update(), pk=med_id, is_active=True)
        src = get_object_or_404(StorageLocation, pk=src_id)
        dst = get_object_or_404(StorageLocation, pk=dst_id)
        if src.kind != StorageLocation.KIND_SHELF or dst.kind != StorageLocation.KIND_SHELF:
            return JsonResponse({'ok': False, 'error': 'Both locations must be storage locations.'}, status=400)

        def at(loc):   # batches whose effective location is `loc`
            return Q(location=loc) | Q(location__isnull=True, medicine__location=loc)

        src_batches = list(
            med.batches.select_for_update().filter(quantity__gt=0).filter(at(src))
            .order_by(F('expiry_date').asc(nulls_last=True), 'received_at')
        )
        available = sum(b.quantity for b in src_batches)
        if qty > available:
            return JsonResponse({'ok': False,
                'error': f'Only {available} unit(s) of "{med.medicine_name}" are stored in {src.code}.'}, status=400)

        total = med.stock_quantity          # unchanged by a transfer
        remaining = qty
        for b in src_batches:               # FEFO: earliest expiry moves first
            if remaining <= 0:
                break
            take = min(b.quantity, remaining)
            dest = (med.batches.filter(batch_number=b.batch_number, expiry_date=b.expiry_date)
                    .filter(at(dst)).exclude(pk=b.pk).first())
            if dest:
                dest.quantity += take
                dest.save()
            else:
                dest = MedicineStock.objects.create(
                    medicine=med, batch_number=b.batch_number, expiry_date=b.expiry_date,
                    quantity=take, purchase_price=b.purchase_price, supplier=b.supplier,
                    location=dst, created_by=request.user,
                )
                MedicineStock.objects.filter(pk=dest.pk).update(received_at=b.received_at)
            b.quantity -= take
            b.save()
            remaining -= take

            StockMovement.objects.create(
                medicine=med, batch=dest, movement_type='transfer',
                quantity=0, quantity_before=total, quantity_after=total,
                batch_number=b.batch_number, expiry_date=b.expiry_date,
                reference_no=f'TRF {src.code}>{dst.code}',
                notes=f'Transferred {take} unit(s) from {src.code} to {dst.code}' + (f' — {notes}' if notes else ''),
                performed_by=request.user,
            )

        # If nothing is left in the old main location, the main location follows the stock
        left_at_src = available - qty
        moved_primary = False
        if med.location_id == src.pk and left_at_src == 0:
            med.location = dst
            med.save(update_fields=['location', 'updated_at'])
            moved_primary = True

    msg = f'Transferred {qty} unit(s) of "{med.medicine_name}" from {src.code} to {dst.code}. Total stock unchanged ({total}).'
    if moved_primary:
        msg += f' Main location is now {dst.code}.'
    messages.success(request, msg)
    return JsonResponse({'ok': True, 'moved': qty, 'total': total})


# ── Areas ────────────────────────────────────────────────────────────────────
@require('storage_locations', 'edit')
@require_POST
def storage_area_create(request):
    name = request.POST.get('name', '').strip()
    if not name:
        messages.error(request, 'Area name is required.')
        return redirect('storage_locations')
    if StorageArea.objects.filter(name__iexact=name).exists():
        messages.error(request, f'An area named "{name}" already exists.')
        return redirect('storage_locations')
    w, h  = STORAGE_ORIENTATIONS.get(request.POST.get('orientation'), STORAGE_ORIENTATIONS['landscape'])
    order = (StorageArea.objects.order_by('-sort_order').values_list('sort_order', flat=True).first() or 0) + 1
    area  = StorageArea.objects.create(
        name=name, description=request.POST.get('description', '').strip(),
        canvas_w=w, canvas_h=h, sort_order=order,
    )
    messages.success(request, f'Area "{area.name}" added. Add storage locations to build its floor plan.')
    return redirect(_storage_url(area.pk))


@require('storage_locations', 'edit')
@require_POST
def storage_area_update(request, pk):
    area = get_object_or_404(StorageArea, pk=pk)
    name = request.POST.get('name', '').strip()
    if not name:
        messages.error(request, 'Area name is required.')
    elif StorageArea.objects.filter(name__iexact=name).exclude(pk=area.pk).exists():
        messages.error(request, f'An area named "{name}" already exists.')
    else:
        area.name = name
        area.description = request.POST.get('description', '').strip()
        area.save()
        messages.success(request, f'Area "{area.name}" updated.')
    return redirect(_storage_url(area.pk))


@require(level='admin')
@require_POST
def storage_area_delete(request, pk):
    area = get_object_or_404(StorageArea, pk=pk)
    if any(_loc_has_stock(l) for l in area.locations.all()):
        messages.error(request, f'Cannot remove "{area.name}" — medicines are still assigned to its '
                                'locations. Reassign them first.')
        return redirect(_storage_url(area.pk))
    name, n = area.name, area.locations.count()
    area.delete()
    messages.success(request, f'Area "{name}" and its {n} location(s) removed.')
    return redirect('storage_locations')


# ── Locations ────────────────────────────────────────────────────────────────
def _default_slot(area):
    n = area.locations.count() % 6
    return {'x': 5 + n * 3, 'y': 5 + n * 3, 'w': 16, 'h': 12}


@require('storage_locations', 'edit')
@require_POST
def storage_location_create(request):
    area = StorageArea.objects.filter(pk=_to_int(request.POST.get('area')) or 0).first()
    if not area:
        messages.error(request, 'Please choose an area.')
        return redirect('storage_locations')
    code = request.POST.get('code', '').strip()[:30]
    if not code:
        messages.error(request, 'Location code is required.')
        return redirect(_storage_url(area.pk))
    if StorageLocation.objects.filter(code__iexact=code).exists():
        messages.error(request, f'A location with code "{code}" already exists.')
        return redirect(_storage_url(area.pk))
    kind = request.POST.get('kind')
    if kind not in dict(StorageLocation.KIND_CHOICES):
        kind = StorageLocation.KIND_SHELF
    loc = StorageLocation.objects.create(
        area=area, code=code, name=request.POST.get('name', '').strip()[:80],
        kind=kind, notes=request.POST.get('notes', '').strip(), **_default_slot(area),
    )
    messages.success(request, f'Location "{loc.code}" added — drag it into place, then Save Layout.')
    return redirect(_storage_url(area.pk, edit=1, focus=loc.pk))


@require('storage_locations', 'edit')
@require_POST
def storage_location_update(request, pk):
    loc  = get_object_or_404(StorageLocation, pk=pk)
    code = request.POST.get('code', '').strip()[:30]
    if not code:
        messages.error(request, 'Location code is required.')
        return redirect(_storage_url(loc.area_id))
    if StorageLocation.objects.filter(code__iexact=code).exclude(pk=loc.pk).exists():
        messages.error(request, f'A location with code "{code}" already exists.')
        return redirect(_storage_url(loc.area_id))

    kind = request.POST.get('kind')
    if kind not in dict(StorageLocation.KIND_CHOICES):
        kind = loc.kind
    if kind == StorageLocation.KIND_LANDMARK and _loc_has_stock(loc):
        messages.error(request, f'"{loc.code}" still holds medicines — reassign them before making it a landmark.')
        return redirect(_storage_url(loc.area_id))

    new_area = StorageArea.objects.filter(pk=_to_int(request.POST.get('area')) or 0).first() or loc.area
    if new_area.pk != loc.area_id:
        loc.area = new_area
        for k, v in _default_slot(new_area).items():
            setattr(loc, k, v)

    loc.code  = code
    loc.name  = request.POST.get('name', '').strip()[:80]
    loc.kind  = kind
    loc.notes = request.POST.get('notes', '').strip()
    loc.save()
    messages.success(request, f'Location "{loc.code}" updated.')
    return redirect(_storage_url(loc.area_id, focus=loc.pk))


@require(level='admin')
@require_POST
def storage_location_delete(request, pk):
    loc = get_object_or_404(StorageLocation, pk=pk)
    area_id, code = loc.area_id, loc.code
    n = len(_placements().get(loc.pk, {}))
    loc.delete()                       # Medicine.location is SET_NULL
    msg = f'Location "{code}" removed.'
    if n:
        msg += f' {n} medicine(s) are now unassigned.'
    messages.success(request, msg)
    return redirect(_storage_url(area_id))


@require('storage_locations', 'edit', ajax=True)
@require_POST
def storage_location_layout(request):
    """Bulk-save dragged / resized shelf positions and canvas sizes (JSON)."""
    try:
        payload = json.loads(request.body or b'{}')
        items   = payload.get('positions', []) or []
        parsed  = {int(i['id']): i for i in items}
        canvases = payload.get('canvases', []) or []
    except (ValueError, KeyError, TypeError, AttributeError):
        return JsonResponse({'ok': False, 'error': 'Invalid data.'}, status=400)

    # ── Shelf positions ──────────────────────────────────────────────────────
    changed = []
    for loc in StorageLocation.objects.filter(pk__in=parsed.keys()):
        it = parsed[loc.pk]
        try:
            w = min(max(float(it['w']), 3.0), 100.0)
            h = min(max(float(it['h']), 3.0), 100.0)
            x = min(max(float(it['x']), 0.0), 100.0 - w)
            y = min(max(float(it['y']), 0.0), 100.0 - h)
        except (KeyError, TypeError, ValueError):
            continue
        loc.x, loc.y, loc.w, loc.h = round(x, 2), round(y, 2), round(w, 2), round(h, 2)
        changed.append(loc)
    if changed:
        StorageLocation.objects.bulk_update(changed, ['x', 'y', 'w', 'h'])

    # ── Canvas (whole floor plan) sizes ──────────────────────────────────────
    canvas_saved = 0
    for c in canvases:
        try:
            area_id = int(c['id'])
            cw = min(max(int(c['w']), 360), 2400)
            ch = min(max(int(c['h']), 240), 1800)
        except (KeyError, TypeError, ValueError):
            continue
        canvas_saved += StorageArea.objects.filter(pk=area_id).update(canvas_w=cw, canvas_h=ch)

    return JsonResponse({'ok': True, 'saved': len(changed), 'canvases': canvas_saved})




VALID_REASONS = dict(MedicineReturn.REASON_CHOICES)
SOURCE_LABELS = dict(MedicineReturn.SOURCE_CHOICES)
 
 
# ─────────────────────────────────────────────────────────────────────────────
# helpers
# ─────────────────────────────────────────────────────────────────────────────
 
def _returned_so_far(dispensing_item):
    return (MedicineReturnItem.objects
            .filter(dispensing_item=dispensing_item)
            .aggregate(t=Sum('quantity'))['t'] or 0)
 
 
def _to_int_or_none(v):
    try:
        return int(v) if v not in (None, '') else None
    except (ValueError, TypeError):
        return None
 
 
def _get_batch(medicine, batch_id):
    """Validate a user-picked batch. None → automatic placement."""
    batch_id = _to_int_or_none(batch_id)
    if not batch_id:
        return None
    try:
        return MedicineStock.objects.select_for_update().get(pk=batch_id, medicine=medicine)
    except MedicineStock.DoesNotExist:
        raise ValueError(
            f'The selected batch does not belong to "{medicine.medicine_name}".')
 
 
def _batches_payload(medicine_ids):
    """{medicine_id: [batch dicts]} — in-stock batches first, then empty ones (max 25 each)."""
    today = timezone.localdate()
    out = {}
    qs = (MedicineStock.objects.filter(medicine_id__in=medicine_ids)
          .select_related('location')
          .order_by('medicine_id', F('expiry_date').asc(nulls_last=True), 'received_at'))
    for b in qs:
        lst = out.setdefault(b.medicine_id, [])
        lst.append({
            'pk':           b.pk,
            'batch_number': b.batch_number or '—',
            'expiry':       b.expiry_date.strftime('%b %d, %Y') if b.expiry_date else 'No expiry',
            'expired':      bool(b.expiry_date and b.expiry_date < today),
            'quantity':     b.quantity,
            'location':     b.location.code if b.location else '',
        })
    for mid, lst in out.items():
        lst.sort(key=lambda x: x['quantity'] == 0)      # stable: in-stock first
        out[mid] = lst[:25]
    return out
 
 
def _restock(medicine, qty, user, reference_no, notes,
             dispensing_item=None, target_batch=None):
    """
    Put `qty` units back on the shelf and write 'return' stock movements.
    Returns [(batch, units_placed), ...].
 
    Priority:
      1. target_batch  — the batch the user picked, gets everything.
      2. the batches the units were originally dispensed from (traced via the
         TXN-<id> 'out' movements), so expiry / cost stay correct.
      3. anything left → a new batch with no expiry.
    """
    placed = []
 
    def _add_to(batch, units, ref):
        before = medicine.stock_quantity
        batch.quantity += units
        batch.save()
        StockMovement.objects.create(
            medicine=medicine, batch=batch, movement_type='return',
            quantity=units, quantity_before=before,
            quantity_after=medicine.stock_quantity,
            batch_number=batch.batch_number, expiry_date=batch.expiry_date,
            purchase_price=batch.purchase_price, supplier=batch.supplier,
            reference_no=ref, notes=notes, performed_by=user,
        )
        placed.append((batch, units))
 
    # 1) user-picked batch
    if target_batch is not None:
        _add_to(target_batch, qty, reference_no)
        return placed
 
    remaining = qty
 
    # 2) original batches
    if dispensing_item is not None:
        txn_ref = f'TXN-{dispensing_item.dispensing_id}'
        taken = {}
        for mv in StockMovement.objects.filter(
                medicine=medicine, movement_type='out',
                reference_no=txn_ref, batch__isnull=False).order_by('id'):
            taken[mv.batch_id] = taken.get(mv.batch_id, 0) + abs(mv.quantity)
 
        put_back = {}
        for mv in StockMovement.objects.filter(
                medicine=medicine, movement_type='return',
                reference_no=f'RET-{txn_ref}', batch__isnull=False):
            put_back[mv.batch_id] = put_back.get(mv.batch_id, 0) + mv.quantity
 
        for batch_id, took in taken.items():
            if remaining <= 0:
                break
            room = took - put_back.get(batch_id, 0)
            if room <= 0:
                continue
            give  = min(room, remaining)
            batch = MedicineStock.objects.select_for_update().get(pk=batch_id)
            _add_to(batch, give, f'RET-{txn_ref}')
            remaining -= give
 
    # 3) leftovers → new blank batch
    if remaining > 0:
        before = medicine.stock_quantity
        batch = MedicineStock.objects.create(
            medicine=medicine, quantity=remaining, created_by=user)
        StockMovement.objects.create(
            medicine=medicine, batch=batch, movement_type='return',
            quantity=remaining, quantity_before=before,
            quantity_after=medicine.stock_quantity,
            reference_no=reference_no, notes=notes, performed_by=user,
        )
        placed.append((batch, remaining))
 
    return placed
 
 
def _record_items(ret, medicine, qty, dispensing_item, placed):
    """One MedicineReturnItem per batch touched (or one batch-less row if not restocked)."""
    if not placed:
        MedicineReturnItem.objects.create(
            medicine_return=ret, medicine=medicine,
            dispensing_item=dispensing_item, quantity=qty)
        return
    for batch, units in placed:
        MedicineReturnItem.objects.create(
            medicine_return=ret, medicine=medicine, dispensing_item=dispensing_item,
            quantity=units, batch=batch,
            batch_number=batch.batch_number, expiry_date=batch.expiry_date)
 
 
def _parse_body(request):
    try:
        return json.loads(request.body or b'{}')
    except (ValueError, TypeError):
        return None
 
 
def _parse_iso(value, default):
    try:
        return date.fromisoformat(value)
    except (ValueError, TypeError):
        return default
 
 
# ─────────────────────────────────────────────────────────────────────────────
# PAGE
# ─────────────────────────────────────────────────────────────────────────────
 
@require('returns')
def returns_view(request):
    return render(request, 'returns.html', {
        'reasons': MedicineReturn.REASON_CHOICES,
        'sources': MedicineReturn.SOURCE_CHOICES,
        'now':     timezone.localtime(timezone.now()),
    })
 
 
# ─────────────────────────────────────────────────────────────────────────────
# Batches of one medicine (Tab 2 batch picker)
# URL: /returns/medicine/<pk>/batches/
# ─────────────────────────────────────────────────────────────────────────────
 
@require('returns', ajax=True)
@require_GET
def return_medicine_batches(request, pk):
    return JsonResponse({'batches': _batches_payload([pk]).get(pk, [])})
 
 
# ─────────────────────────────────────────────────────────────────────────────
# TAB 1 — patient's dispensed medicines (AJAX)
# URL: /returns/patient/<hpatkey>/items/
# ─────────────────────────────────────────────────────────────────────────────
 
@require('returns', ajax=True)
@require_GET
def return_patient_items(request, hpatkey):
    hpatkey = str(hpatkey).strip()
 
    txns = Dispensing.objects.filter(ihomis_hpatkey=hpatkey)
    if not txns.exists():
        try:
            hpercode = HPerson.objects.using('ihomis_plus').get(pk=hpatkey).hospital_number
            txns = Dispensing.objects.filter(ihomis_hpercode=hpercode)
        except Exception:
            txns = Dispensing.objects.none()
 
    items = list(DispensingItem.objects
                 .filter(dispensing__in=txns)
                 .select_related('medicine', 'dispensing', 'dispensing__doctor')
                 .order_by('-dispensing__created_at', 'pk'))
 
    returned_map = dict(
        MedicineReturnItem.objects.filter(dispensing_item__in=items)
        .values_list('dispensing_item')
        .annotate(t=Sum('quantity'))
    )
 
    rows = []
    for it in items:
        returned = returned_map.get(it.pk, 0)
        m, t     = it.medicine, it.dispensing
        rows.append({
            'item_pk':         it.pk,
            'txn_pk':          t.pk,
            'date':            timezone.localtime(t.created_at).strftime('%b %d, %Y'),
            'time':            timezone.localtime(t.created_at).strftime('%I:%M %p'),
            'prescription_no': t.prescription_no or '',
            'doctor':          f'Dr. {t.doctor.get_full_name()}' if t.doctor else '',
            'medicine_pk':     m.pk,
            'name':            m.medicine_name,
            'generic':         m.generic_name,
            'strength':        m.strength,
            'form':            m.get_dosage_form_display(),
            'is_s2':           m.is_s2_regulated,
            'dispensed':       it.quantity,
            'returned':        returned,
            'returnable':      max(it.quantity - returned, 0),
            'unit_price':      float(it.unit_price),
        })
 
    return JsonResponse({'items': rows})
 
 
# ─────────────────────────────────────────────────────────────────────────────
# TAB 1 — submit return for a patient
# ─────────────────────────────────────────────────────────────────────────────
 
@require('returns', 'edit', ajax=True)
@require_POST
def return_patient_submit(request):
    data = _parse_body(request)
    if data is None:
        return JsonResponse({'error': 'Invalid data.'}, status=400)

    reason       = data.get('reason', '')
    notes        = str(data.get('notes', '') or '').strip()
    hpatkey      = str(data.get('ihomis_hpatkey', '') or '').strip()
    hpercode     = str(data.get('ihomis_hpercode', '') or '').strip()
    patient_name = str(data.get('patient_name', '') or '').strip()

    if reason not in VALID_REASONS:
        return JsonResponse({'error': 'Please select a reason for return.'}, status=400)
    if not hpatkey:
        return JsonResponse({'error': 'No patient selected.'}, status=400)

    # item_pk -> quantity wanted (no batch choice: units go back to the batch they came from)
    wanted = {}
    try:
        for row in data.get('items', []):
            q = int(row.get('quantity', 0))
            if q > 0:
                pk = int(row['item_pk'])
                wanted[pk] = wanted.get(pk, 0) + q
    except (ValueError, TypeError, KeyError):
        return JsonResponse({'error': 'Invalid items.'}, status=400)
    if not wanted:
        return JsonResponse({'error': 'Enter a quantity to return for at least one medicine.'}, status=400)

    try:
        with transaction.atomic():
            lines = list(DispensingItem.objects.select_for_update()
                         .filter(pk__in=wanted.keys()))
            if len(lines) != len(wanted):
                return JsonResponse({'error': 'Some dispensed items were not found.'}, status=400)

            for it in lines:
                txn = it.dispensing
                if txn.ihomis_hpatkey != hpatkey and (not hpercode or txn.ihomis_hpercode != hpercode):
                    return JsonResponse({'error': 'An item does not belong to this patient.'}, status=400)
                avail = it.quantity - _returned_so_far(it)
                if wanted[it.pk] > avail:
                    return JsonResponse({
                        'error': f'Cannot return {wanted[it.pk]} of "{it.medicine.medicine_name}" '
                                 f'(Txn #{txn.pk}). Only {avail} still returnable.'
                    }, status=400)

            ret = MedicineReturn.objects.create(
                source=MedicineReturn.SOURCE_PATIENT,
                patient_name=patient_name, ihomis_hpatkey=hpatkey, ihomis_hpercode=hpercode,
                reason=reason, notes=notes, restocked=True, processed_by=request.user,
            )
            total_units = 0
            for it in lines:
                qty    = wanted[it.pk]
                placed = _restock(
                    it.medicine, qty, request.user,
                    reference_no=f'RET-{ret.pk}',
                    notes=f'Return #{ret.pk} — {VALID_REASONS[reason]}'
                          + (f' — {notes}' if notes else ''),
                    dispensing_item=it,      # auto-return to the ORIGINAL batch(es)
                )
                _record_items(ret, it.medicine, qty, it, placed)
                total_units += qty
    except Exception as e:
        return JsonResponse({'error': str(e)}, status=400)

    return JsonResponse({
        'success': True, 'return_pk': ret.pk, 'units': total_units,
        'message': f'Return #{ret.pk} recorded — {total_units} unit(s) returned to stock.',
    })
 
 
# ─────────────────────────────────────────────────────────────────────────────
# TAB 2 — free-form "Log Returned Medicines"
# ─────────────────────────────────────────────────────────────────────────────
 
@require('returns', 'edit', ajax=True)
@require_POST
def return_log_submit(request):
    data = _parse_body(request)
    if data is None:
        return JsonResponse({'error': 'Invalid data.'}, status=400)
 
    reason       = data.get('reason', '')
    notes        = str(data.get('notes', '') or '').strip()
    patient_name = str(data.get('patient_name', '') or '').strip()
    restock      = bool(data.get('restock', False))
 
    if reason not in VALID_REASONS:
        return JsonResponse({'error': 'Please select a reason for return.'}, status=400)
 
    # key = (medicine_pk, batch_id or None) so two different batches stay separate
    wanted = {}
    try:
        for row in data.get('items', []):
            q = int(row.get('quantity', 0))
            if q > 0:
                key = (int(row['medicine_pk']),
                       _to_int_or_none(row.get('batch_id')) if restock else None)
                wanted[key] = wanted.get(key, 0) + q
    except (ValueError, TypeError, KeyError):
        return JsonResponse({'error': 'Invalid medicine lines.'}, status=400)
    if not wanted:
        return JsonResponse({'error': 'Add at least one medicine with a quantity.'}, status=400)
 
    try:
        with transaction.atomic():
            med_ids = {k[0] for k in wanted}
            meds = {m.pk: m for m in Medicine.objects.filter(pk__in=med_ids, is_active=True)}
            if len(meds) != len(med_ids):
                return JsonResponse({'error': 'A selected medicine was not found.'}, status=400)
 
            ret = MedicineReturn.objects.create(
                source=MedicineReturn.SOURCE_LOG, patient_name=patient_name,
                reason=reason, notes=notes, restocked=restock,
                processed_by=request.user,
            )
            for (mpk, batch_id), qty in wanted.items():
                med = meds[mpk]
                placed = []
                if restock:
                    placed = _restock(
                        med, qty, request.user,
                        reference_no=f'RET-{ret.pk}',
                        notes=f'Return Log #{ret.pk} — {VALID_REASONS[reason]}'
                              + (f' — {patient_name}' if patient_name else '')
                              + (f' — {notes}' if notes else ''),
                        target_batch=_get_batch(med, batch_id),
                    )
                _record_items(ret, med, qty, None, placed)
    except Exception as e:
        return JsonResponse({'error': str(e)}, status=400)
 
    units = sum(wanted.values())
    return JsonResponse({
        'success': True, 'return_pk': ret.pk, 'units': units,
        'message': f'Return log #{ret.pk} saved — {len(med_ids)} medicine(s), {units} unit(s)'
                   + (' added back to stock.' if restock else '.'),
    })
 
 
# ─────────────────────────────────────────────────────────────────────────────
# REPORT — every returned medicine (JSON for the on-screen table + print)
# URL: /returns/report/data/?date_from=&date_to=&reason=&source=&q=
# ─────────────────────────────────────────────────────────────────────────────
 
@require('returns', ajax=True)
@require_GET
def returns_report_data(request):
    today     = timezone.localdate()
    date_from = _parse_iso(request.GET.get('date_from'), today.replace(day=1))
    date_to   = _parse_iso(request.GET.get('date_to'), today)
    reason    = request.GET.get('reason', '')
    source    = request.GET.get('source', '')
    q         = request.GET.get('q', '').strip()
 
    qs = (MedicineReturnItem.objects
          .filter(medicine_return__returned_at__date__gte=date_from,
                  medicine_return__returned_at__date__lte=date_to)
          .select_related('medicine', 'medicine_return', 'medicine_return__processed_by'))
    if reason in VALID_REASONS:
        qs = qs.filter(medicine_return__reason=reason)
    if source in SOURCE_LABELS:
        qs = qs.filter(medicine_return__source=source)
    if q:
        qs = qs.filter(
            Q(medicine_return__patient_name__icontains=q) |
            Q(medicine_return__ihomis_hpercode__icontains=q) |
            Q(medicine__medicine_name__icontains=q) |
            Q(medicine__generic_name__icontains=q) |
            Q(batch_number__icontains=q))
    qs = qs.order_by('-medicine_return__returned_at', 'pk')
 
    total_units    = qs.aggregate(t=Sum('quantity'))['t'] or 0
    restocked_units = qs.filter(medicine_return__restocked=True).aggregate(t=Sum('quantity'))['t'] or 0
    return_count   = qs.values('medicine_return').distinct().count()
    line_count     = qs.count()
 
    LIMIT = 3000
    rows = []
    for it in qs[:LIMIT]:
        r, m = it.medicine_return, it.medicine
        when = timezone.localtime(r.returned_at)
        by   = r.processed_by
        rows.append({
            'return_pk':    r.pk,
            'date':         when.strftime('%b %d, %Y'),
            'time':         when.strftime('%I:%M %p'),
            'source':       SOURCE_LABELS.get(r.source, r.source),
            'patient_name': r.patient_name or '—',
            'hn':           r.ihomis_hpercode or '',
            'medicine':     m.medicine_name,
            'generic':      m.generic_name,
            'strength':     m.strength,
            'form':         m.get_dosage_form_display(),
            'batch_number': it.batch_number or ('—' if r.restocked else 'Not restocked'),
            'expiry':       it.expiry_date.strftime('%b %d, %Y') if it.expiry_date else '—',
            'quantity':     it.quantity,
            'reason':       VALID_REASONS.get(r.reason, r.reason),
            'restocked':    r.restocked,
            'by':           (by.get_full_name() or by.username) if by else '—',
            'notes':        r.notes or '',
        })
 
    return JsonResponse({
        'rows': rows,
        'truncated': line_count > LIMIT,
        'totals': {
            'returns': return_count, 'lines': line_count,
            'units': total_units, 'restocked_units': restocked_units,
        },
        'filters': {
            'date_from': date_from.isoformat(), 'date_to': date_to.isoformat(),
        },
    })



# ─────────────────────────────────────────────────────────────────────────────
# STOCK CARD  (Appendix 127 — one medicine per card, portrait Folio 8.5 x 13)
# ─────────────────────────────────────────────────────────────────────────────

STOCK_CARD_HOSPITAL     = 'MAYOR HILARION A. RAMIRO, SR. MEDICAL CENTER'
STOCK_CARD_ROWS_OPTIONS = (25, 30, 35)
STOCK_CARD_UNITS = {
    'tablet': 'tablet', 'capsule': 'capsule', 'syrup': 'bottle', 'suspension': 'bottle',
    'injection': 'ampule/vial', 'cream': 'tube', 'ointment': 'tube', 'drops': 'bottle',
    'inhaler': 'piece', 'patch': 'piece', 'suppository': 'piece', 'powder': 'sachet',
    'other': 'piece',
}


def _stock_card_bucket(d):
    """5-day period inside the month: 1-5, 6-10, 11-15, 16-20, 21-25, 26-end."""
    start = min((d.day - 1) // 5, 5) * 5 + 1
    last  = calendar.monthrange(d.year, d.month)[1]
    end   = last if start == 26 else start + 4
    return start, end


def _stock_card_build(med, date_from, date_to, rows_per_page):
    """Builds the paged stock card for ONE medicine (issuances grouped per 5 days)."""
    start_dt = timezone.make_aware(datetime.combine(date_from, datetime.min.time()))
    end_dt   = timezone.make_aware(datetime.combine(date_to + timedelta(days=1), datetime.min.time()))

    prev = (StockMovement.objects.filter(medicine=med, created_at__lt=start_dt)
            .order_by('-created_at', '-id').first())
    opening = prev.quantity_after if prev else 0

    moves = list(StockMovement.objects
                 .filter(medicine=med, created_at__gte=start_dt, created_at__lt=end_dt)
                 .exclude(movement_type='transfer')
                 .select_related('supplier')
                 .order_by('created_at', 'id'))

    txn_ids = {int(m.reference_no[4:]) for m in moves
               if m.reference_no.startswith('TXN-') and m.reference_no[4:].isdigit()}
    txns = Dispensing.objects.select_related('ward').in_bulk(txn_ids)

    def patient_name(m):
        t = txns.get(int(m.reference_no[4:])) if m.reference_no[4:].isdigit() else None
        if t:
            return (t.customer_name or '').strip() or (t.ward.name if t.ward else 'Walk-in')
        return (m.dispensed_to or '').strip() or 'Walk-in'

    def other_out_label(m):
        tag = {'expired': 'Expired', 'damaged': 'Damaged', 'adjust': 'Adjustment',
               'audit': 'Audit adj.'}.get(m.movement_type, 'Stock out')
        return ' — '.join(p for p in (tag, m.dispensed_to, m.notes) if p)

    def receipt_lines(m):
        if m.movement_type == 'return':
            return ['Return'] + ([m.reference_no] if m.reference_no else [])
        if m.movement_type in ('adjust', 'audit'):
            return [' — '.join(p for p in ('Adjustment', m.notes) if p)]
        lines = [m.supplier.name if m.supplier else 'Received']
        if m.reference_no:
            lines.append(f'DR No.: {m.reference_no}')
        if m.purchase_price:
            lines.append(f'Price: {m.purchase_price:,.2f}')
        return lines

    rows, buckets = [], {}
    for m in moves:
        if not m.quantity:
            continue
        d = timezone.localtime(m.created_at).date()

        if m.quantity > 0:                                   # RECEIPT
            rows.append({'date_s': d.strftime('%m/%d/%Y'), 'order': m.created_at, 'kind': 'in',
                         'qty': m.quantity, 'expiry': m.expiry_date,
                         'names': receipt_lines(m), 'et_al': False})
            continue

        if m.reference_no.startswith('TXN-'):                # ISSUANCE → 5-day bucket
            s, e = _stock_card_bucket(d)
            key = (d.year, d.month, s)
            r = buckets.get(key)
            if r is None:
                r = {'date_s': f'{d.month}/{s}-{e}/{d.year % 100}', 'order': m.created_at,
                     'kind': 'out', 'qty': 0, 'expiry': None, 'names': [], 'et_al': False}
                buckets[key] = r
                rows.append(r)
            r['qty'] += -m.quantity
            r['order'] = m.created_at
            nm = patient_name(m)
            if not r['names']:
                r['names'] = [nm]                            # one representative only
            elif nm != r['names'][0]:
                r['et_al'] = True
        else:                                                # expired / damaged / adjust
            rows.append({'date_s': d.strftime('%m/%d/%Y'), 'order': m.created_at, 'kind': 'out',
                         'qty': -m.quantity, 'expiry': None,
                         'names': [other_out_label(m)], 'et_al': False})

    rows.sort(key=lambda r: r['order'])

    bal = opening
    for r in rows:
        bal += r['qty'] if r['kind'] == 'in' else -r['qty']
        ref = '\n'.join(r['names'])
        if r['et_al']:
            ref += '; et al'
        r['ref']     = ref
        r['balance'] = bal
        r['exp_s']   = r['expiry'].strftime('%m/%d/%Y') if r['expiry'] else ''
        r['in_qty']  = r['qty'] if r['kind'] == 'in'  else ''
        r['out_qty'] = r['qty'] if r['kind'] == 'out' else ''

    cap    = rows_per_page - 1            # 1 row per page is the Price / balance-forwarded row
    chunks = [rows[i:i + cap] for i in range(0, len(rows), cap)] or [[]]
    pages, carry = [], opening
    for n, chunk in enumerate(chunks, 1):
        pages.append({'number': n, 'total': len(chunks), 'forwarded': carry,
                      'rows': chunk, 'blank_rows': range(max(cap - len(chunk), 0))})
        if chunk:
            carry = chunk[-1]['balance']

    price = med.purchase_price or med.selling_price
    desc  = f'{med.generic_name or med.medicine_name} {med.strength}'.strip()
    if med.brand_name:
        desc += f' ({med.brand_name})'
    return {
        'med': med, 'pages': pages, 'opening': opening, 'closing': bal,
        'has_events': bool(rows),
        'price': f'{price:,.2f}',
        'unit': STOCK_CARD_UNITS.get(med.dosage_form, 'piece'),
        'stock_no': med.barcode or f'MED-{med.pk:04d}',
        'description': desc,
        'reorder': med.reorder_level,
    }


def _stock_card_context(request):
    today     = timezone.localdate()
    date_from = _parse_iso(request.GET.get('date_from'), today.replace(day=1))
    date_to   = _parse_iso(request.GET.get('date_to'), today)
    if date_to < date_from:
        date_from, date_to = date_to, date_from

    try:
        rows_per_page = int(request.GET.get('rows', 35))
    except (TypeError, ValueError):
        rows_per_page = 35
    if rows_per_page not in STOCK_CARD_ROWS_OPTIONS:
        rows_per_page = 35

    meds      = Medicine.objects.filter(is_active=True).order_by('medicine_name')
    med_param = request.GET.get('medicine', '')
    cards = []
    if med_param == 'all':
        for m in meds:
            c = _stock_card_build(m, date_from, date_to, rows_per_page)
            if c['has_events'] or c['opening']:
                cards.append(c)
    elif _to_int(med_param):
        m = meds.filter(pk=_to_int(med_param)).first()
        if m:
            cards.append(_stock_card_build(m, date_from, date_to, rows_per_page))

    return {
        'medicines': meds, 'cards': cards, 'med_param': med_param,
        'date_from': date_from, 'date_to': date_to,
        'rows_per_page': rows_per_page, 'rows_options': STOCK_CARD_ROWS_OPTIONS,
        'hospital_name': STOCK_CARD_HOSPITAL,
    }


def _stock_card_tag(ctx):
    return (re.sub(r'[^A-Za-z0-9]+', '_', ctx['cards'][0]['med'].generic_name).strip('_')
            if len(ctx['cards']) == 1 else 'ALL')


@require('stock_card')
def stock_card_report(request):
    return render(request, 'stock_card.html', _stock_card_context(request))


# ── PDF ──────────────────────────────────────────────────────────────────────
@require('stock_card')
def stock_card_export_pdf(request):
    if not REPORTLAB_AVAILABLE:
        messages.error(request, 'reportlab is not installed. Run: pip install reportlab')
        return redirect('stock_card_report')
    from reportlab.platypus import PageBreak

    ctx = _stock_card_context(request)
    if not ctx['cards']:
        messages.error(request, 'Select a medicine to generate its Stock Card.')
        return redirect('stock_card_report')

    FOLIO   = (8.5 * inch, 13 * inch)                 # portrait Folio
    margin  = 0.5 * inch
    avail_w = FOLIO[0] - 2 * margin
    black   = colors.black

    buffer = io.BytesIO()
    doc = SimpleDocTemplate(buffer, pagesize=FOLIO, leftMargin=margin, rightMargin=margin,
                            topMargin=margin, bottomMargin=margin,
                            title='Stock Card', author='MHARSMC ePIS')

    ss       = getSampleStyleSheet()
    base     = ParagraphStyle('b', parent=ss['Normal'], fontName='Helvetica', fontSize=8, leading=10)
    cell     = ParagraphStyle('c', parent=base, alignment=TA_CENTER)
    cell_l   = ParagraphStyle('cl', parent=base, alignment=TA_LEFT)
    bold_c   = ParagraphStyle('bc', parent=cell, fontName='Helvetica-Bold')
    head     = ParagraphStyle('h', parent=cell, fontName='Helvetica-Bold', fontSize=8.5)
    hosp     = ParagraphStyle('ho', parent=base, fontName='Helvetica-Bold', fontSize=13, leading=16,
                              alignment=TA_CENTER)
    title    = ParagraphStyle('t', parent=base, fontName='Helvetica-Bold', fontSize=12, leading=15,
                              alignment=TA_CENTER)
    sub      = ParagraphStyle('s', parent=base, fontSize=8.5, alignment=TA_CENTER,
                              textColor=colors.HexColor('#444444'))
    right    = ParagraphStyle('r', parent=base, fontSize=9, alignment=TA_RIGHT)
    lab      = ParagraphStyle('lab', parent=base, fontSize=9, leading=12)
    price_st = ParagraphStyle('p', parent=cell_l, textColor=colors.HexColor('#C00000'))

    def P(text, st=cell):
        return Paragraph(_xml_escape(str(text)).replace('\n', '<br/>'), st)

    fr    = [12, 34, 12, 14, 14, 14]                  # Date | Reference | R.Qty | Exp | I.Qty | Balance
    col_w = [avail_w * f / 100 for f in fr]

    story, first = [], True
    for card in ctx['cards']:
        for pg in card['pages']:
            if not first:
                story.append(PageBreak())
            first = False

            story.append(Paragraph('Appendix 127', right))
            story.append(Spacer(1, 4))

            top = Table([[Paragraph('<u>' + _xml_escape(ctx['hospital_name']) + '</u>', hosp)],
                         [Paragraph('STOCK CARD', title)],
                         [Paragraph(f'{ctx["date_from"]:%B %d, %Y} – {ctx["date_to"]:%B %d, %Y}'
                                    f'  |  Page {pg["number"]} of {pg["total"]}', sub)]],
                        colWidths=[avail_w])
            top.setStyle(TableStyle([('BOX', (0, 0), (-1, -1), 0.8, black),
                                     ('TOPPADDING', (0, 0), (-1, -1), 3),
                                     ('BOTTOMPADDING', (0, 0), (-1, -1), 3)]))
            story.append(top)

            info = Table([
                [Paragraph(f'<b>ITEM / DESCRIPTION:</b> {_xml_escape(card["description"])}', lab),
                 Paragraph(f'<b>STOCK NO.:</b> {_xml_escape(card["stock_no"])}', lab)],
                [Paragraph(f'<b>UNIT:</b> {_xml_escape(card["unit"])}', lab),
                 Paragraph(f'<b>RE-ORDER POINT:</b> {card["reorder"]}', lab)],
            ], colWidths=[avail_w * 0.62, avail_w * 0.38])
            info.setStyle(TableStyle([('GRID', (0, 0), (-1, -1), 0.8, black),
                                      ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
                                      ('TOPPADDING', (0, 0), (-1, -1), 6),
                                      ('BOTTOMPADDING', (0, 0), (-1, -1), 6)]))
            story.append(info)

            data = [
                [P('Date', head), P('Reference', head), P('Receipt', head), '',
                 P('Issuance', head), P('Balance', head)],
                ['', '', P('Qty', head), P('Expiration<br/>Date', head), P('Qty', head), ''],
            ]
            # Price / balance-forwarded row
            data.append(['', P(f'Price: {card["price"]}', price_st), '', '', '',
                         P(f'{pg["forwarded"]:,}', bold_c)])
            for r in pg['rows']:
                data.append([P(r['date_s']), P(r['ref'], cell_l),
                             P(f'{r["in_qty"]:,}') if r['in_qty'] != '' else '',
                             P(r['exp_s']),
                             P(f'{r["out_qty"]:,}') if r['out_qty'] != '' else '',
                             P(f'{r["balance"]:,}', bold_c)])
            blanks = len(pg['blank_rows'])
            data += [[''] * 6 for _ in range(blanks)]

            heights = [None] * (3 + len(pg['rows'])) + [0.26 * inch] * blanks
            tbl = Table(data, colWidths=col_w, rowHeights=heights, repeatRows=2)
            tbl.setStyle(TableStyle([
                ('GRID', (0, 0), (-1, -1), 0.7, black),
                ('SPAN', (0, 0), (0, 1)), ('SPAN', (1, 0), (1, 1)),
                ('SPAN', (2, 0), (3, 0)), ('SPAN', (5, 0), (5, 1)),
                ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
                ('TOPPADDING', (0, 0), (-1, -1), 5), ('BOTTOMPADDING', (0, 0), (-1, -1), 5),
                ('LEFTPADDING', (0, 0), (-1, -1), 4), ('RIGHTPADDING', (0, 0), (-1, -1), 4),
            ]))
            story.append(tbl)

    doc.build(story)
    resp = HttpResponse(buffer.getvalue(), content_type='application/pdf')
    resp['Content-Disposition'] = (
        f'attachment; filename="StockCard_{_stock_card_tag(ctx)}_{ctx["date_from"]}_{ctx["date_to"]}.pdf"')
    return resp


# ── Excel ────────────────────────────────────────────────────────────────────
@require('stock_card')
def stock_card_export_excel(request):
    if not OPENPYXL_AVAILABLE:
        messages.error(request, 'openpyxl is not installed. Run: pip install openpyxl')
        return redirect('stock_card_report')
    from openpyxl.worksheet.pagebreak import Break
    from openpyxl.worksheet.properties import PageSetupProperties

    ctx = _stock_card_context(request)
    if not ctx['cards']:
        messages.error(request, 'Select a medicine to generate its Stock Card.')
        return redirect('stock_card_report')

    thin    = Side(style='thin', color='000000')
    box     = Border(left=thin, right=thin, top=thin, bottom=thin)
    f_norm  = Font(name='Arial', size=9)
    f_bold  = Font(name='Arial', size=9, bold=True)
    f_head  = Font(name='Arial', size=10, bold=True)
    f_hosp  = Font(name='Arial', size=14, bold=True, underline='single')
    f_ttl   = Font(name='Arial', size=12, bold=True)
    f_price = Font(name='Arial', size=9, color='C00000')
    ctr = Alignment(horizontal='center', vertical='center', wrap_text=True)
    lft = Alignment(horizontal='left',   vertical='center', wrap_text=True)
    rgt = Alignment(horizontal='right',  vertical='center')
    widths = [14, 42, 12, 15, 12, 13]

    wb = openpyxl.Workbook()
    wb.remove(wb.active)
    used = set()

    for card in ctx['cards']:
        ws = wb.create_sheet(_xl_sheet_title(card['med'], used))
        for i, w in enumerate(widths, 1):
            ws.column_dimensions[get_column_letter(i)].width = w
        ws.page_setup.orientation = 'portrait'
        ws.page_setup.paperSize   = 14                 # Folio 8.5 x 13 in
        ws.page_setup.fitToWidth  = 1
        ws.page_setup.fitToHeight = 0
        ws.sheet_properties.pageSetUpPr = PageSetupProperties(fitToPage=True)
        ws.page_margins.left = ws.page_margins.right = 0.5
        ws.page_margins.top  = ws.page_margins.bottom = 0.5
        ws.print_options.horizontalCentered = True

        r = 1
        for pg in card['pages']:
            _xl_merge(ws, r, 1, r, 6, 'Appendix 127', Font(name='Arial', size=9), rgt); r += 1
            _xl_merge(ws, r, 1, r, 6, ctx['hospital_name'], f_hosp, ctr, box)
            ws.row_dimensions[r].height = 24; r += 1
            _xl_merge(ws, r, 1, r, 6, 'STOCK CARD', f_ttl, ctr, box); r += 1
            _xl_merge(ws, r, 1, r, 6,
                      f'{ctx["date_from"]:%B %d, %Y} – {ctx["date_to"]:%B %d, %Y}'
                      f'   |   Page {pg["number"]} of {pg["total"]}',
                      Font(name='Arial', size=8, color='555555'), ctr, box); r += 1

            _xl_merge(ws, r, 1, r, 4, f'ITEM / DESCRIPTION:  {card["description"]}', f_bold, lft, box)
            _xl_merge(ws, r, 5, r, 6, f'STOCK NO.:  {card["stock_no"]}', f_bold, lft, box)
            ws.row_dimensions[r].height = 30; r += 1
            _xl_merge(ws, r, 1, r, 4, f'UNIT:  {card["unit"]}', f_bold, lft, box)
            _xl_merge(ws, r, 5, r, 6, f'RE-ORDER POINT:  {card["reorder"]}', f_bold, lft, box)
            ws.row_dimensions[r].height = 22; r += 1

            # column headers
            _xl_merge(ws, r, 1, r + 1, 1, 'Date', f_head, ctr, box)
            _xl_merge(ws, r, 2, r + 1, 2, 'Reference', f_head, ctr, box)
            _xl_merge(ws, r, 3, r, 4, 'Receipt', f_head, ctr, box)
            _xl_merge(ws, r, 5, r, 5, 'Issuance', f_head, ctr, box)
            _xl_merge(ws, r, 6, r + 1, 6, 'Balance', f_head, ctr, box)
            for c, label in ((3, 'Qty'), (4, 'Expiration\nDate'), (5, 'Qty')):
                cell = ws.cell(r + 1, c, label)
                cell.font, cell.alignment, cell.border = f_head, ctr, box
            ws.row_dimensions[r].height = 20
            ws.row_dimensions[r + 1].height = 32
            r += 2

            # Price / balance-forwarded row
            for c in range(1, 7):
                ws.cell(r, c).border = box
            ws.cell(r, 2, f'Price: {card["price"]}').font = f_price
            ws.cell(r, 2).alignment = lft
            b = ws.cell(r, 6, pg['forwarded'])
            b.font, b.alignment, b.number_format = f_bold, ctr, '#,##0'
            ws.row_dimensions[r].height = 20
            r += 1

            # data rows
            for rw in pg['rows']:
                vals = [rw['date_s'], rw['ref'], rw['in_qty'], rw['exp_s'], rw['out_qty'], rw['balance']]
                for c, v in enumerate(vals, 1):
                    cell = ws.cell(r, c, v)
                    cell.font      = f_bold if c == 6 else f_norm
                    cell.border    = box
                    cell.alignment = lft if c == 2 else ctr
                    if c in (3, 5, 6):
                        cell.number_format = '#,##0'
                ws.row_dimensions[r].height = max(20, 12.5 * _xl_lines(rw['ref'], widths[1]) + 6)
                r += 1
            for _ in pg['blank_rows']:
                for c in range(1, 7):
                    ws.cell(r, c).border = box
                ws.row_dimensions[r].height = 20
                r += 1

            ws.row_breaks.append(Break(id=r))          # next card page starts on a new printed page
            r += 2

    resp = HttpResponse(
        content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
    resp['Content-Disposition'] = (
        f'attachment; filename="StockCard_{_stock_card_tag(ctx)}_{ctx["date_from"]}_{ctx["date_to"]}.xlsx"')
    wb.save(resp)
    return resp


# ─────────────────────────────────────────────────────────────────────────────
# SUPPLIES LEDGER CARD  (Accounting Form No. 84 — portrait Folio 8.5 x 13)
# Reuses STOCK_CARD_UNITS, STOCK_CARD_HOSPITAL, _stock_card_bucket,
# _stock_card_tag, _xl_merge, _xl_lines and _xl_sheet_title from earlier code.
# ─────────────────────────────────────────────────────────────────────────────
from decimal import Decimal

SUPPLIES_LEDGER_ROWS_OPTIONS = (30, 35, 40)


def _supplies_ledger_build(med, date_from, date_to, rows_per_page, group):
    """One medicine → paged ledger with Receipts / Issues / Balances (qty + cost)."""
    start_dt = timezone.make_aware(datetime.combine(date_from, datetime.min.time()))
    end_dt   = timezone.make_aware(datetime.combine(date_to + timedelta(days=1), datetime.min.time()))
    zero     = Decimal('0')
    cent     = Decimal('0.01')
    fallback = Decimal(str(med.purchase_price or med.selling_price or 0))

    def price_of(p):
        return Decimal(str(p)) if p else fallback

    # ── Opening balance (qty + cost) before the period ───────────────────────
    prev = (StockMovement.objects.filter(medicine=med, created_at__lt=start_dt)
            .order_by('-created_at', '-id').first())
    opening_qty  = prev.quantity_after if prev else 0
    opening_cost = zero
    for q, p in (StockMovement.objects.filter(medicine=med, created_at__lt=start_dt)
                 .values_list('quantity', 'purchase_price')):
        opening_cost += q * price_of(p)
    if opening_qty <= 0 or opening_cost < 0:
        opening_cost = zero

    moves = list(StockMovement.objects
                 .filter(medicine=med, created_at__gte=start_dt, created_at__lt=end_dt)
                 .exclude(movement_type='transfer')
                 .select_related('supplier')
                 .order_by('created_at', 'id'))

    txn_ids = {int(m.reference_no[4:]) for m in moves
               if m.reference_no.startswith('TXN-') and m.reference_no[4:].isdigit()}
    txns = Dispensing.objects.select_related('ward').in_bulk(txn_ids)

    def patient_name(m):
        t = txns.get(int(m.reference_no[4:])) if m.reference_no[4:].isdigit() else None
        if t:
            return (t.customer_name or '').strip() or (t.ward.name if t.ward else 'Walk-in')
        return (m.dispensed_to or '').strip() or 'Walk-in'

    def other_out_label(m):
        tag = {'expired': 'Expired', 'damaged': 'Damaged', 'adjust': 'Adjustment',
               'audit': 'Audit adj.'}.get(m.movement_type, 'Stock out')
        return ' — '.join(p for p in (tag, m.dispensed_to, m.notes) if p)

    def receipt_lines(m):
        if m.movement_type == 'return':
            return ['Return'] + ([m.reference_no] if m.reference_no else [])
        if m.movement_type in ('adjust', 'audit'):
            return [' — '.join(p for p in ('Adjustment', m.notes) if p)]
        lines = [m.supplier.name if m.supplier else 'Received']
        if m.reference_no:
            lines.append(f'DR No.: {m.reference_no}')
        return lines

    def issue_key_and_date(m, d):
        if group == '0':
            return ('m', m.pk), d.strftime('%m/%d/%Y')
        if group == '1':
            return ('d', d), d.strftime('%m/%d/%Y')
        s, e = _stock_card_bucket(d)
        return ('b', d.year, d.month, s), f'{d.month}/{s}-{e}/{d.year % 100}'

    rows, buckets = [], {}
    for m in moves:
        if not m.quantity:
            continue
        d = timezone.localtime(m.created_at).date()
        price = price_of(m.purchase_price)

        if m.quantity > 0:                                       # RECEIPT
            rows.append({'date_s': d.strftime('%m/%d/%Y'), 'order': m.created_at, 'kind': 'in',
                         'qty': m.quantity, 'unit': price, 'cost': m.quantity * price,
                         'names': receipt_lines(m), 'et_al': False})
            continue

        if m.reference_no.startswith('TXN-'):                    # ISSUE (dispensing)
            key, date_s = issue_key_and_date(m, d)
            r = buckets.get(key)
            if r is None:
                r = {'date_s': date_s, 'order': m.created_at, 'kind': 'out',
                     'qty': 0, 'unit': None, 'cost': zero, 'names': [], 'et_al': False}
                buckets[key] = r
                rows.append(r)
            r['qty']  += -m.quantity
            r['cost'] += (-m.quantity) * price
            r['order'] = m.created_at
            nm = patient_name(m)
            if not r['names']:
                r['names'] = [nm]                                # one representative only
            elif nm != r['names'][0]:
                r['et_al'] = True
        else:                                                    # expired / damaged / adjust
            rows.append({'date_s': d.strftime('%m/%d/%Y'), 'order': m.created_at, 'kind': 'out',
                         'qty': -m.quantity, 'unit': None, 'cost': (-m.quantity) * price,
                         'names': [other_out_label(m)], 'et_al': False})

    rows.sort(key=lambda r: r['order'])

    bal_qty, bal_cost = opening_qty, opening_cost
    for r in rows:
        r['cost'] = r['cost'].quantize(cent)
        if r['kind'] == 'in':
            bal_qty  += r['qty']
            bal_cost += r['cost']
        else:
            bal_qty  -= r['qty']
            bal_cost -= r['cost']
        if bal_qty == 0:
            bal_cost = zero
        ref = '\n'.join(r['names'])
        if r['et_al']:
            ref += '; et al'
        r['ref']       = ref
        r['bal_qty']   = bal_qty
        r['bal_cost']  = bal_cost
        r['in_qty']    = r['qty']  if r['kind'] == 'in' else ''
        r['in_unit']   = r['unit'] if r['kind'] == 'in' else ''
        r['in_total']  = r['cost'] if r['kind'] == 'in' else ''
        r['out_qty']   = r['qty']  if r['kind'] == 'out' else ''
        r['out_total'] = r['cost'] if r['kind'] == 'out' else ''

    # Each page: 1 "Balance Forwarded" row + data rows + blanks + 1 "Balance Carried Forward" row
    cap    = rows_per_page - 2
    chunks = [rows[i:i + cap] for i in range(0, len(rows), cap)] or [[]]
    pages, fq, fc = [], opening_qty, opening_cost
    for n, chunk in enumerate(chunks, 1):
        cq, cc = (chunk[-1]['bal_qty'], chunk[-1]['bal_cost']) if chunk else (fq, fc)
        pages.append({'number': n, 'total': len(chunks),
                      'fwd_qty': fq, 'fwd_cost': fc,
                      'rows': chunk, 'blank_rows': range(max(cap - len(chunk), 0)),
                      'cf_qty': cq, 'cf_cost': cc})
        fq, fc = cq, cc

    desc = ' '.join(p for p in (med.generic_name, med.strength, med.get_dosage_form_display()) if p)
    if med.brand_name:
        desc += f' ({med.brand_name})'
    return {
        'med': med, 'pages': pages, 'has_events': bool(rows), 'opening': opening_qty,
        'item': med.medicine_name, 'description': desc,
        'unit': STOCK_CARD_UNITS.get(med.dosage_form, 'piece'),
        'stock_no': f'MED-{med.pk:04d}', 'code': med.barcode or '',
    }


def _supplies_ledger_context(request):
    today     = timezone.localdate()
    date_from = _parse_iso(request.GET.get('date_from'), today.replace(day=1))
    date_to   = _parse_iso(request.GET.get('date_to'), today)
    if date_to < date_from:
        date_from, date_to = date_to, date_from

    try:
        rows_per_page = int(request.GET.get('rows', 35))
    except (TypeError, ValueError):
        rows_per_page = 35
    if rows_per_page not in SUPPLIES_LEDGER_ROWS_OPTIONS:
        rows_per_page = 35

    group = request.GET.get('group', '5')
    if group not in ('5', '1', '0'):
        group = '5'

    meds      = Medicine.objects.filter(is_active=True).order_by('medicine_name')
    med_param = request.GET.get('medicine', '')
    cards = []
    if med_param == 'all':
        for m in meds:
            c = _supplies_ledger_build(m, date_from, date_to, rows_per_page, group)
            if c['has_events'] or c['opening']:
                cards.append(c)
    elif _to_int(med_param):
        m = meds.filter(pk=_to_int(med_param)).first()
        if m:
            cards.append(_supplies_ledger_build(m, date_from, date_to, rows_per_page, group))

    return {
        'medicines': meds, 'cards': cards, 'med_param': med_param,
        'date_from': date_from, 'date_to': date_to,
        'rows_per_page': rows_per_page, 'rows_options': SUPPLIES_LEDGER_ROWS_OPTIONS,
        'group': group, 'hospital_name': STOCK_CARD_HOSPITAL,
    }


@require('supplies_ledger')
def supplies_ledger_report(request):
    return render(request, 'supplies_ledger.html', _supplies_ledger_context(request))


# ── PDF ──────────────────────────────────────────────────────────────────────
@require('supplies_ledger')
def supplies_ledger_export_pdf(request):
    if not REPORTLAB_AVAILABLE:
        messages.error(request, 'reportlab is not installed. Run: pip install reportlab')
        return redirect('supplies_ledger_report')
    from reportlab.platypus import PageBreak

    ctx = _supplies_ledger_context(request)
    if not ctx['cards']:
        messages.error(request, 'Select a medicine to generate its Supplies Ledger Card.')
        return redirect('supplies_ledger_report')

    FOLIO   = (8.5 * inch, 13 * inch)
    margin  = 0.5 * inch
    avail_w = FOLIO[0] - 2 * margin
    black   = colors.black

    buffer = io.BytesIO()
    doc = SimpleDocTemplate(buffer, pagesize=FOLIO, leftMargin=margin, rightMargin=margin,
                            topMargin=margin, bottomMargin=margin,
                            title='Supplies Ledger Card', author='MHARSMC ePIS')

    ss     = getSampleStyleSheet()
    base   = ParagraphStyle('b', parent=ss['Normal'], fontName='Helvetica', fontSize=7.5, leading=9)
    cell   = ParagraphStyle('c', parent=base, alignment=TA_CENTER)
    cell_l = ParagraphStyle('cl', parent=base, alignment=TA_LEFT)
    numst  = ParagraphStyle('n', parent=base, alignment=TA_RIGHT)
    numb   = ParagraphStyle('nb', parent=numst, fontName='Helvetica-Bold')
    ital   = ParagraphStyle('i', parent=base, fontName='Helvetica-Oblique', fontSize=8.5, leading=10)
    head   = ParagraphStyle('h', parent=cell, fontName='Helvetica-Bold', fontSize=8)
    title  = ParagraphStyle('t', parent=base, fontName='Helvetica-Bold', fontSize=14, leading=17,
                            alignment=TA_CENTER)
    meta   = ParagraphStyle('m', parent=base, fontSize=7, leading=9, alignment=TA_CENTER,
                            textColor=colors.HexColor('#444444'))
    lab    = ParagraphStyle('lab', parent=base, fontSize=9, leading=11)
    small  = ParagraphStyle('sm', parent=base, fontSize=8.5, leading=10.5)
    right  = ParagraphStyle('r', parent=small, alignment=TA_RIGHT)

    def P(text, st=cell):
        return Paragraph(_xml_escape(str(text)).replace('\n', '<br/>'), st)

    def qty(v):
        return '' if v == '' else f'{v:,}'

    def money(v):
        return '' if v == '' else f'{v:,.2f}'

    # Date | Reference | R.Qty | R.Unit | R.Total | I.Qty | I.Total | B.Qty | B.Total
    fr    = [9, 27, 7, 9, 10, 7, 10, 8, 13]
    col_w = [avail_w * f / 100 for f in fr]

    story, first = [], True
    for card in ctx['cards']:
        for pg in card['pages']:
            if not first:
                story.append(PageBreak())
            first = False

            top = Table([[P('Accounting Form No. 84\nRevised January 1992', small),
                          Paragraph(f'Stock No.: <u>&nbsp;{_xml_escape(card["stock_no"])}&nbsp;</u>', right)]],
                        colWidths=[avail_w * 0.6, avail_w * 0.4])
            top.setStyle(TableStyle([('LEFTPADDING', (0, 0), (-1, -1), 0),
                                     ('RIGHTPADDING', (0, 0), (-1, -1), 0)]))
            story += [top, Spacer(1, 4)]

            period = (f'{ctx["hospital_name"]}\n{ctx["date_from"]:%B %d, %Y} – {ctx["date_to"]:%B %d, %Y}'
                      f'  |  Page {pg["number"]} of {pg["total"]}')
            data = [
                [P('SUPPLIES LEDGER CARD', title), '', '', '', '', P(period, meta), '', '', ''],
                [Paragraph(f'<b>Item</b>&nbsp;&nbsp;{_xml_escape(card["item"])}', lab), '', '', '', '',
                 Paragraph(f'<b>Code</b>&nbsp;&nbsp;{_xml_escape(card["code"])}', lab), '', '', ''],
                [Paragraph(f'<b>Description</b>&nbsp;&nbsp;{_xml_escape(card["description"])}', lab), '', '', '', '',
                 Paragraph(f'<b>Unit</b>&nbsp;&nbsp;{_xml_escape(card["unit"])}', lab), '', '', ''],
                [P('Date', head), P('Reference', head), P('RECEIPTS', head), '', '',
                 P('ISSUES', head), '', P('BALANCES', head), ''],
                ['', '', P('Qty.', head), P('Unit\nCost', head), P('Total\nCost', head),
                 P('Qty.', head), P('Total\nCost', head), P('Qty.', head), P('Total\nCost', head)],
                ['', P('Balance Forwarded', ital), '', '', '', '', '',
                 P(qty(pg['fwd_qty']), numb), P(money(pg['fwd_cost']), numb)],
            ]
            for r in pg['rows']:
                data.append([P(r['date_s']), P(r['ref'], cell_l),
                             P(qty(r['in_qty']), numst), P(money(r['in_unit']), numst),
                             P(money(r['in_total']), numst),
                             P(qty(r['out_qty']), numst), P(money(r['out_total']), numst),
                             P(qty(r['bal_qty']), numb), P(money(r['bal_cost']), numb)])
            blanks = len(pg['blank_rows'])
            data += [[''] * 9 for _ in range(blanks)]
            data.append([P('Balance Carried Forward', ital), '', '', '', '', '', '',
                         P(qty(pg['cf_qty']), numb), P(money(pg['cf_cost']), numb)])
            last = len(data) - 1

            heights = [None] * (6 + len(pg['rows'])) + [0.22 * inch] * blanks + [0.3 * inch]
            tbl = Table(data, colWidths=col_w, rowHeights=heights)
            tbl.setStyle(TableStyle([
                ('GRID', (0, 0), (-1, -1), 0.7, black),
                ('SPAN', (0, 0), (4, 0)), ('SPAN', (5, 0), (8, 0)),
                ('SPAN', (0, 1), (4, 1)), ('SPAN', (5, 1), (8, 1)),
                ('SPAN', (0, 2), (4, 2)), ('SPAN', (5, 2), (8, 2)),
                ('SPAN', (0, 3), (0, 4)), ('SPAN', (1, 3), (1, 4)),
                ('SPAN', (2, 3), (4, 3)), ('SPAN', (5, 3), (6, 3)), ('SPAN', (7, 3), (8, 3)),
                ('SPAN', (0, last), (1, last)),
                ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
                ('TOPPADDING', (0, 0), (-1, -1), 3), ('BOTTOMPADDING', (0, 0), (-1, -1), 3),
                ('LEFTPADDING', (0, 0), (-1, -1), 3), ('RIGHTPADDING', (0, 0), (-1, -1), 3),
                ('TOPPADDING', (0, 0), (-1, 0), 6), ('BOTTOMPADDING', (0, 0), (-1, 0), 6),
            ]))
            story.append(tbl)

    doc.build(story)
    resp = HttpResponse(buffer.getvalue(), content_type='application/pdf')
    resp['Content-Disposition'] = (
        f'attachment; filename="SuppliesLedger_{_stock_card_tag(ctx)}_{ctx["date_from"]}_{ctx["date_to"]}.pdf"')
    return resp


# ── Excel ────────────────────────────────────────────────────────────────────
@require('supplies_ledger')
def supplies_ledger_export_excel(request):
    if not OPENPYXL_AVAILABLE:
        messages.error(request, 'openpyxl is not installed. Run: pip install openpyxl')
        return redirect('supplies_ledger_report')
    from openpyxl.worksheet.pagebreak import Break
    from openpyxl.worksheet.properties import PageSetupProperties

    ctx = _supplies_ledger_context(request)
    if not ctx['cards']:
        messages.error(request, 'Select a medicine to generate its Supplies Ledger Card.')
        return redirect('supplies_ledger_report')

    thin   = Side(style='thin', color='000000')
    box    = Border(left=thin, right=thin, top=thin, bottom=thin)
    f_norm = Font(name='Arial', size=9)
    f_bold = Font(name='Arial', size=9, bold=True)
    f_head = Font(name='Arial', size=9, bold=True)
    f_ttl  = Font(name='Arial', size=14, bold=True)
    f_ital = Font(name='Arial', size=9, italic=True)
    f_sml  = Font(name='Arial', size=8, color='555555')
    ctr = Alignment(horizontal='center', vertical='center', wrap_text=True)
    lft = Alignment(horizontal='left',   vertical='center', wrap_text=True)
    rgt = Alignment(horizontal='right',  vertical='center')
    widths = [11, 34, 9, 11, 13, 9, 13, 9, 14]
    QTY, MONEY = '#,##0', '#,##0.00'

    wb = openpyxl.Workbook()
    wb.remove(wb.active)
    used = set()

    def put(ws, r, c, v, font=f_norm, align=ctr, fmt=None):
        cell = ws.cell(r, c, v)
        cell.font, cell.alignment, cell.border = font, align, box
        if fmt and v != '':
            cell.number_format = fmt
        return cell

    for card in ctx['cards']:
        ws = wb.create_sheet(_xl_sheet_title(card['med'], used))
        for i, w in enumerate(widths, 1):
            ws.column_dimensions[get_column_letter(i)].width = w
        ws.page_setup.orientation = 'portrait'
        ws.page_setup.paperSize   = 14                 # Folio 8.5 x 13 in
        ws.page_setup.fitToWidth  = 1
        ws.page_setup.fitToHeight = 0
        ws.sheet_properties.pageSetUpPr = PageSetupProperties(fitToPage=True)
        ws.page_margins.left = ws.page_margins.right = 0.5
        ws.page_margins.top  = ws.page_margins.bottom = 0.5
        ws.print_options.horizontalCentered = True

        r = 1
        for pg in card['pages']:
            _xl_merge(ws, r, 1, r, 5, 'Accounting Form No. 84\nRevised January 1992', Font(name='Arial', size=9), lft)
            _xl_merge(ws, r, 6, r, 9, f'Stock No.:  {card["stock_no"]}', f_bold, rgt)
            ws.row_dimensions[r].height = 28; r += 1

            _xl_merge(ws, r, 1, r, 5, 'SUPPLIES LEDGER CARD', f_ttl, ctr, box)
            _xl_merge(ws, r, 6, r, 9,
                      f'{ctx["hospital_name"]}\n{ctx["date_from"]:%B %d, %Y} – {ctx["date_to"]:%B %d, %Y}'
                      f'  |  Page {pg["number"]} of {pg["total"]}', f_sml, ctr, box)
            ws.row_dimensions[r].height = 36; r += 1

            _xl_merge(ws, r, 1, r, 5, f'Item:  {card["item"]}', f_bold, lft, box)
            _xl_merge(ws, r, 6, r, 9, f'Code:  {card["code"]}', f_bold, lft, box)
            ws.row_dimensions[r].height = 22; r += 1
            _xl_merge(ws, r, 1, r, 5, f'Description:  {card["description"]}', f_bold, lft, box)
            _xl_merge(ws, r, 6, r, 9, f'Unit:  {card["unit"]}', f_bold, lft, box)
            ws.row_dimensions[r].height = 24; r += 1

            # column headers
            _xl_merge(ws, r, 1, r + 1, 1, 'Date', f_head, ctr, box)
            _xl_merge(ws, r, 2, r + 1, 2, 'Reference', f_head, ctr, box)
            _xl_merge(ws, r, 3, r, 5, 'RECEIPTS', f_head, ctr, box)
            _xl_merge(ws, r, 6, r, 7, 'ISSUES', f_head, ctr, box)
            _xl_merge(ws, r, 8, r, 9, 'BALANCES', f_head, ctr, box)
            for c, label in enumerate(['Qty.', 'Unit\nCost', 'Total\nCost', 'Qty.', 'Total\nCost',
                                       'Qty.', 'Total\nCost'], 3):
                put(ws, r + 1, c, label, f_head)
            ws.row_dimensions[r].height = 18
            ws.row_dimensions[r + 1].height = 30
            r += 2

            # Balance forwarded
            for c in range(1, 10):
                put(ws, r, c, '')
            put(ws, r, 2, 'Balance Forwarded', f_ital, lft)
            put(ws, r, 8, pg['fwd_qty'], f_bold, ctr, QTY)
            put(ws, r, 9, float(pg['fwd_cost']), f_bold, ctr, MONEY)
            ws.row_dimensions[r].height = 20
            r += 1

            # data rows
            for rw in pg['rows']:
                put(ws, r, 1, rw['date_s'])
                put(ws, r, 2, rw['ref'], f_norm, lft)
                put(ws, r, 3, rw['in_qty'], f_norm, ctr, QTY)
                put(ws, r, 4, float(rw['in_unit']) if rw['in_unit'] != '' else '', f_norm, ctr, MONEY)
                put(ws, r, 5, float(rw['in_total']) if rw['in_total'] != '' else '', f_norm, ctr, MONEY)
                put(ws, r, 6, rw['out_qty'], f_norm, ctr, QTY)
                put(ws, r, 7, float(rw['out_total']) if rw['out_total'] != '' else '', f_norm, ctr, MONEY)
                put(ws, r, 8, rw['bal_qty'], f_bold, ctr, QTY)
                put(ws, r, 9, float(rw['bal_cost']), f_bold, ctr, MONEY)
                ws.row_dimensions[r].height = max(20, 12.5 * _xl_lines(rw['ref'], widths[1]) + 6)
                r += 1
            for _ in pg['blank_rows']:
                for c in range(1, 10):
                    ws.cell(r, c).border = box
                ws.row_dimensions[r].height = 18
                r += 1

            # Balance carried forward
            for c in range(1, 10):
                ws.cell(r, c).border = box
            _xl_merge(ws, r, 1, r, 2, 'Balance Carried Forward', f_ital, lft, box)
            put(ws, r, 8, pg['cf_qty'], f_bold, ctr, QTY)
            put(ws, r, 9, float(pg['cf_cost']), f_bold, ctr, MONEY)
            ws.row_dimensions[r].height = 24
            r += 1

            ws.row_breaks.append(Break(id=r))          # next card page starts on a new printed page
            r += 2

    resp = HttpResponse(
        content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
    resp['Content-Disposition'] = (
        f'attachment; filename="SuppliesLedger_{_stock_card_tag(ctx)}_{ctx["date_from"]}_{ctx["date_to"]}.xlsx"')
    wb.save(resp)
    return resp