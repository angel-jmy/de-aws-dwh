from flask import Flask, jsonify, render_template_string, request
import boto3, json, random, datetime

app = Flask(__name__)

region = "us-east-2"
stream_name = "team2_Data_Fact"
kinesis = boto3.client("kinesis", region_name=region)

products = [
    # --- Electronics ---
    {"sku": "ELEC-001", "name": "Wireless Mouse", "price": 25.99},
    {"sku": "ELEC-002", "name": "Bluetooth Headphones", "price": 59.99},
    {"sku": "ELEC-003", "name": "Bluetooth Speaker", "price": 45.50},
    {"sku": "ELEC-004", "name": "Smartphone Stand", "price": 14.99},
    {"sku": "ELEC-005", "name": "USB-C Hub", "price": 39.99},

    # --- Home Appliances ---
    {"sku": "HOME-001", "name": "Coffee Maker", "price": 89.99},
    {"sku": "HOME-002", "name": "Air Fryer", "price": 129.50},
    {"sku": "HOME-003", "name": "Electric Kettle", "price": 34.99},
    {"sku": "HOME-004", "name": "Vacuum Cleaner", "price": 159.00},

    # --- Fashion / Accessories ---
    {"sku": "FASH-001", "name": "Smart Watch", "price": 199.99},
    {"sku": "FASH-002", "name": "Leather Wallet", "price": 49.99},
    {"sku": "FASH-003", "name": "Sunglasses", "price": 79.99},
]


# --- HTML 页面 ---
HTML = """
<!DOCTYPE html>
<html>
<head>
  <title>🛒 TechMart Simulator</title>
  <style>
    body { font-family: Arial; text-align: center; margin-top: 50px; }
    button { font-size: 18px; margin: 8px; padding: 10px 25px; border-radius: 8px; }
    textarea { width: 80%; height: 150px; margin-top: 20px; font-family: monospace; }
    #output { margin-top: 30px; font-family: monospace; white-space: pre-wrap; color: #333; }
    .error { color: red; }
  </style>
  <script>
    async function send(path) {
      try {
        const res = await fetch(path);
        const data = await res.json();
        document.getElementById("output").textContent = JSON.stringify(data, null, 2);
        document.getElementById("output").className = "";
      } catch (err) {
        document.getElementById("output").textContent = "Error: " + err.message;
        document.getElementById("output").className = "error";
      }
    }

    async function sendOrder() {
      const input = document.getElementById("customInput").value;
      try {
        const res = await fetch('/custom', {
          method: 'POST',
          headers: {'Content-Type': 'application/json'},
          body: input
        });
        
        if (!res.ok) {
          const errorText = await res.text();
          throw new Error(`Server error: ${res.status} - ${errorText}`);
        }
        
        const data = await res.json();
        document.getElementById("output").textContent = JSON.stringify(data, null, 2);
        document.getElementById("output").className = "";
      } catch (err) {
        document.getElementById("output").textContent = "Error: " + err.message;
        document.getElementById("output").className = "error";
      }
    }
  </script>
</head>
<body>
  <h1>🛍️ TechMart Order Generator</h1>
  <button onclick="send('/order')">Generate Order</button>
  <h3>🧪 Custom JSON Input</h3>
 <textarea id="customInput">{
  "event_type": "order_created",
  "order_id": "O-1234",
  "customer_id": "C-105",
  "items": [
    { 
      "sku": "ELEC-001", 
      "name": "Wireless Mouse", 
      "price": 25.99,
      "quantity": 2,
      "line_total": 51.98
    },
    { 
      "sku": "HOME-002", 
      "name": "Air Fryer", 
      "price": 129.50,
      "quantity": 1,
      "line_total": 129.50
    }
  ],
  "total": 181.48,
  "currency": "USD",
  "created_at": "2025-11-12T17:40:00Z"
}</textarea>
<br>
  <button onclick="sendOrder()">Send Order JSON</button>
  <div id="output"></div>
</body>
</html>
"""

@app.route("/")
def home():
    return render_template_string(HTML)

@app.route("/order")
def send_order():
    try:
        selected_items = random.sample(products, k=random.randint(1, 6))
        items = []
        subtotal = 0.0
        for prod in selected_items:
            qty = random.randint(1, 10)
            line_total = round(prod["price"] * qty, 2)
            items.append({**prod, "quantity": qty, "line_total": line_total})
            subtotal += line_total

        order = {
            "event_type": "order_created",
            "order_id": f"O-{random.randint(1000,9999)}",
            "customer_id": f"C-{random.randint(100,149)}",
            "items": items,
            "total": round(subtotal, 2),
            "currency": "USD",
            "created_at": datetime.datetime.utcnow().isoformat() + "Z",
        }
        kinesis.put_record(StreamName=stream_name, Data=json.dumps(order), PartitionKey=order["customer_id"])
        return jsonify({"status": "success", "order": order})
    except Exception as e:
        return jsonify({"status": "error", "error": str(e)}), 500

@app.route("/custom", methods=["POST"])
def send_custom():
    try:
        raw_data = request.get_data(as_text=True)
        data = json.loads(raw_data)
        kinesis.put_record(
            StreamName=stream_name,
            Data=json.dumps(data),
            PartitionKey=data.get("customer_id", "unknown")
        )
        return jsonify({"status": "success", "data_sent": data})
    except json.JSONDecodeError as e:
        return jsonify({"status": "error", "error": f"Invalid JSON: {str(e)}"}), 400
    except Exception as e:
        return jsonify({"status": "error", "error": str(e)}), 500

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5001, debug=True)
