import numpy as np
from PIL import Image
import cv2
import base64


def decode_base64_to_pil(base64_str: str) -> Image.Image:
    """Decode base64 string to PIL Image (RGB)."""
    img_bytes = base64.b64decode(base64_str)
    # try cv2 then convert to PIL
    nparr = np.frombuffer(img_bytes, np.uint8)
    img_cv = cv2.imdecode(nparr, cv2.IMREAD_COLOR)
    if img_cv is None:
        # fallback: open via PIL
        from io import BytesIO
        return Image.open(BytesIO(img_bytes)).convert("RGB")
    # convert BGR->RGB
    img_rgb = cv2.cvtColor(img_cv, cv2.COLOR_BGR2RGB)
    pil = Image.fromarray(img_rgb)
    return pil