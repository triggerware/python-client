import triggerware as tw

client = tw.TriggerwareClient("localhost", 5221)
rel_data = client.get_rel_data()

# Convert rel_data to a serializable dict before writing to JSON.
import json

def to_serializable(obj):
    if hasattr(obj, "to_dict"):
        return obj.to_dict()
    elif hasattr(obj, "__dict__"):
        return {k: to_serializable(v) for k, v in obj.__dict__.items()}
    elif isinstance(obj, (list, tuple)):
        return [to_serializable(i) for i in obj]
    elif isinstance(obj, dict):
        return {k: to_serializable(v) for k, v in obj.items()}
    else:
        return obj

serializable_rel_data = to_serializable(rel_data)

with open("my_rel_data.json", "w") as f:
    json.dump(serializable_rel_data, f, indent=4)
