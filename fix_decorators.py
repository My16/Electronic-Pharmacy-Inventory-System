import re, shutil

PATH = 'PharmaSys/views.py'
shutil.copy(PATH, PATH + '.bak')          # backup first

A = "level='admin'"
MAP = {
    'inventory_view': "'inventory'",
    'inventory_add_view': "'inventory', 'edit'",
    'inventory_edit_view': "'inventory', 'edit'",
    'inventory_delete_view': A,
    'inventory_barcode_lookup': "('inventory', 'stock_management', 'dispensing'), ajax=True",
    'inventory_stock_adjust': "'inventory', 'edit', ajax=True",
    'medicine_search_ajax': "('inventory', 'stock_management', 'dispensing'), ajax=True",
    'stock_management_view': "'stock_management'",
    'stock_in_view': "'stock_management', 'edit'",
    'stock_out_view': "'stock_management', 'edit'",
    'stock_adjust_view': "'stock_management', 'edit'",
    'stock_medicine_info': "('stock_management', 'inventory'), ajax=True",
    'suppliers_view': "'suppliers'",
    'supplier_add_view': "'suppliers', 'edit'",
    'supplier_edit_view': "'suppliers', 'edit'",
    'supplier_delete_view': A,
    'supplier_toggle_view': "'suppliers', 'edit', ajax=True",
    'supplier_detail_view': "'suppliers', ajax=True",
    'doctors_view': "'doctors'",
    'doctor_add_view': "'doctors', 'edit'",
    'doctor_edit_view': "'doctors', 'edit'",
    'doctor_delete_view': A,
    'doctor_toggle_view': "'doctors', 'edit', ajax=True",
    'doctor_search_ajax': "('doctors', 'dispensing'), ajax=True",
    'doctor_s2_validate_ajax': "('doctors', 'dispensing'), ajax=True",
    'user_management_view': A,
    'user_add_view': A,
    'user_edit_view': A,
    'user_delete_view': A,
    'user_toggle_active_view': "level='admin', ajax=True",
    'reports_view': "'reports'",
    'report_export_excel': "'reports'",
    'report_export_pdf': "'reports'",
    'consumption_report': "'consumption_report'",
    'dispensing_view': "'dispensing'",
    'dispensing_create': "'dispensing', 'edit', ajax=True",
    'dispensing_receipt': "('dispensing', 'dispensing_card'), ajax=True",
    'patient_search_ajax': "('dispensing', 'dispensing_card'), ajax=True",
    'patient_info_ajax': "('dispensing', 'dispensing_card'), ajax=True",
    'dispensing_card_list': "'dispensing_card'",
    'dispensing_card_detail': "'dispensing_card'",
    'link_patient_to_dispensing': "'dispensing', 'edit', ajax=True",
    'category_search_ajax': "('inventory', 'stock_management'), ajax=True",
    'category_create_ajax': "'inventory', 'edit', ajax=True",
    'batch_edit_view': "'stock_management', 'edit', ajax=True",
    'batch_delete_view': "'stock_management', 'edit', ajax=True",
    'access_management_view': A,
    'storage_locations': "'inventory'",
    'storage_location_medicines': "'inventory', ajax=True",
    'storage_find_medicine': "'inventory', ajax=True",
    'storage_area_create': A,
    'storage_area_update': A,
    'storage_area_delete': A,
    'storage_location_create': A,
    'storage_location_update': A,
    'storage_location_delete': A,
    'storage_location_layout': "level='admin', ajax=True",
}

with open(PATH, encoding='utf-8') as f:
    lines = f.read().split('\n')

out = []
for line in lines:
    m = re.match(r'^def (\w+)\(', line)
    if m and m.group(1) in MAP:
        block = []
        while out and out[-1].startswith('@'):
            block.insert(0, out.pop())
        keep = [d for d in block
                if not d.startswith('@login_required')
                and not d.startswith('@user_passes_test')
                and not d.startswith('@require(')]     # keeps @require_POST etc.
        out.append(f"@require({MAP[m.group(1)]})")
        out.extend(keep)
    out.append(line)

with open(PATH, 'w', encoding='utf-8') as f:
    f.write('\n'.join(out))

print('Done. Anything still using old decorators:')
for n, l in enumerate(out, 1):
    if re.search(r'user_passes_test|can_access_module|_inventory_perm|@login_required', l) \
       and not l.startswith(('from ', 'import ')):
        print(n, l)