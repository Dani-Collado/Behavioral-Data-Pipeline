import os
import math
import random
from core.database import PipelineDB

def run_seed():
    print("=== BEHAVIORAL DATA PIPELINE SEEDER ===")
    
    # 1. Initialize Pipeline DB
    db = PipelineDB()
    
    # 2. Add Fully Completed Dataset
    print("[*] Generating dataset 101: 'E-Commerce Checkout Flow'")
    db.guardar_o_actualizar_dataset({
        'dataset_id': 101,
        'nombre': 'E-Commerce Checkout Flow',
        'origen': 'Internal DB',
        'total_events': 5,
        'processed_events': 5,
        'active_hours': 12.5,
        'target_processing_hours': 12.5,
        'automation': 1
    })
    
    eventos_completos = [
        {"id": "add_to_cart", "name": "Item Added to Cart"},
        {"id": "view_cart", "name": "Cart Viewed"},
        {"id": "checkout_started", "name": "Checkout Process Started"},
        {"id": "shipping_selected", "name": "Shipping Method Selected"},
        {"id": "payment_success", "name": "Payment Processed Successfully"}
    ]
    
    for idx, ev in enumerate(eventos_completos, 1):
        db.guardar_event_detail({
            'event_id': ev['id'],
            'dataset_id': 101,
            'display_name': ev['name'],
            'descripcion': f"User triggered the {ev['name']} event.",
            'processed': True
        })
        # Note: Since it's fully processed, we update the timeline just for completion
        db.actualizar_orden_evento(101, ev['id'], idx)
        
    # 3. Add WIP Dataset (User Retention Analysis)
    print("[*] Generating dataset 202: 'User Retention Analysis'")
    target_hours = 45.5
    db.guardar_o_actualizar_dataset({
        'dataset_id': 202,
        'nombre': 'User Retention Analysis',
        'origen': 'App Analytics',
        'total_events': 10,
        'processed_events': 2,
        'active_hours': 5.2,
        'target_processing_hours': target_hours,
        'automation': 1
    })

    eventos_wip = [
        {"id": "account_created", "name": "Account Created"},
        {"id": "first_login", "name": "First Login Completed"},
        {"id": "profile_updated", "name": "Profile Avatar Updated"},
        {"id": "first_purchase", "name": "First Purchase Completed"},
        {"id": "newsletter_sub", "name": "Subscribed to Newsletter"},
        {"id": "friend_invite", "name": "Invited a Friend"},
        {"id": "review_posted", "name": "Posted First Review"},
        {"id": "loyalty_tier_1", "name": "Reached Loyalty Tier 1"},
        {"id": "loyalty_tier_2", "name": "Reached Loyalty Tier 2"},
        {"id": "churn_risk_flag", "name": "Churn Risk Flag Resolved"}
    ]
    
    for idx, ev in enumerate(eventos_wip, 1):
        processed_state = True if idx <= 2 else False # First two are processed
        db.guardar_event_detail({
            'event_id': ev['id'],
            'dataset_id': 202,
            'display_name': ev['name'],
            'descripcion': f"Analytical event mapping for {ev['name']}.",
            'processed': processed_state
        })
        
    # 4. Apply mathematical square root logic to unsequenced events (idx 3-10)
    print("[*] Generating weighted processing timelines via Math.sqrt...")
    timeline_updates = []
    
    eventos_pendientes = db.obtener_eventos_pendientes(202)
    # Target distribution time (subtract processed events proportion roughly)
    remaining_time_to_distribute = (target_hours * 60) * 0.8  
    
    weights = []
    for i in range(len(eventos_pendientes)):
        if i == 0:
            weights.append(0.0) # Fixed first value
        else:
            # Random raw delta in minutes (e.g. from 1 hour to 30 days)
            raw_delta = random.uniform(60, 43200)
            weights.append(math.sqrt(raw_delta))
            
    sum_weights = sum(weights)
    if sum_weights == 0: sum_weights = 1
    
    # Exclude 1.5% for the first item
    scale_factor = (remaining_time_to_distribute * 0.985) / sum_weights
    
    for i, ev_info in enumerate(eventos_pendientes):
        event_id = ev_info['event_id']
        orden = i + 1 
        
        if i == 0:
            wait_time = remaining_time_to_distribute * 0.015
        else:
            wait_time = scale_factor * weights[i]
            
        timeline_updates.append({
            'event_id': event_id,
            'orden': orden,
            'wait_minutes': round(wait_time, 2)
        })
        
    db.guardar_timeline_eventos(202, timeline_updates)
    
    print("[+] Seeding complete! Check your dashboard with: python dashboard_cli.py")

if __name__ == "__main__":
    run_seed()
