import requests
import time
import os

TICKET = "13FA4F65-CAED-4197-95F4-1EC8440E19D6"

def test_sync_performance(rut):
    print(f"Testing real-time sync for RUT: {rut}")
    start = time.time()
    
    # 1. Search OCs
    url_search = f"https://api.mercadopublico.cl/servicios/v1/publico/ordenesdecompra.json?rutproveedor={rut}&ticket={TICKET}"
    try:
        r = requests.get(url_search, timeout=5)
        if r.status_code == 200:
            data = r.json()
            if data.get("Cantidad", 0) > 0:
                oc_code = data["Listado"][0]["Codigo"]
                # 2. Get Details
                url_details = f"https://api.mercadopublico.cl/servicios/v1/publico/OrdenCompra.json?codigo={oc_code}&ticket={TICKET}"
                r_det = requests.get(url_details, timeout=5)
                print(f"Success! Time: {time.time() - start:.2f}s")
                return True
    except Exception as e:
        print(f"Timeout or Error: {e}")
    
    print(f"Failed or slow. Time: {time.time() - start:.2f}s")
    return False

# Test with a common RUT
test_sync_performance("76123456-7")
