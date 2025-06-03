from io import BytesIO
from PIL import Image
from collections import Counter


class image_manipulation():

    @classmethod
    async def _get_highest_frequency_color_hex(cls, image_data):
        image_bytes = BytesIO(image_data)

        image = Image.open(image_bytes)

        image = image.convert('RGB')

        pixels = list(image.getdata())

        color_count = Counter(pixels)

        highest_frequency_color = max(color_count, key=color_count.get)

        hex_color = '#{:02x}{:02x}{:02x}'.format(*highest_frequency_color)

        return hex_color