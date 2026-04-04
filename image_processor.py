"""
Image processor extracted from ImageTemplate (github.com/Arnaszd/ImageTemplate).
Takes an input image and adds a media player overlay (blurred background, album art, controls).
"""

import os
from PIL import Image, ImageDraw, ImageFilter, ImageFont


def get_font(size):
    """Find a suitable font from the system."""
    font_options = [
        "GOTHICB.TTF", "GOTH.TTF", "arial.ttf", "arialbd.ttf",
        "GOTHIC.TTF", "impact.ttf", "IMPACT.TTF"
    ]
    font_dirs = [
        "",
        "C:/Windows/Fonts/",
        "/usr/share/fonts/",
        "/usr/share/fonts/truetype/",
        "/System/Library/Fonts/"
    ]
    for font_name in font_options:
        for font_dir in font_dirs:
            try:
                return ImageFont.truetype(os.path.join(font_dir, font_name), size)
            except (IOError, OSError):
                continue
    return ImageFont.load_default()


def crop_to_square(image):
    """Crop image to 1:1 aspect ratio from center."""
    width, height = image.size
    if width > height:
        left = (width - height) // 2
        return image.crop((left, 0, left + height, height))
    else:
        top = (height - width) // 2
        return image.crop((0, top, width, top + width))


def create_template(image_path, title="", artist="", blur_amount=60):
    """
    Process an image into a TikTok-style music cover with media player overlay.

    Args:
        image_path: Path to input image
        title: Song title text
        artist: Artist name text
        blur_amount: Blur intensity 0-100 (default 60)

    Returns:
        PIL.Image (RGB, 1080x1920)
    """
    target_width = 1080
    target_height = 1920
    target_ratio = 9 / 16

    # --- Background: blurred + darkened ---
    original = Image.open(image_path).convert("RGB")
    width, height = original.size

    if width / height > target_ratio:
        new_w = int(height * target_ratio)
        bg = original.crop(((width - new_w) // 2, 0, (width + new_w) // 2, height))
    else:
        new_h = int(width / target_ratio)
        bg = original.crop((0, (height - new_h) // 2, width, (height + new_h) // 2))

    background = bg.resize((target_width, target_height), Image.LANCZOS)

    if blur_amount > 0:
        small = background.resize((target_width // 2, target_height // 2), Image.LANCZOS)
        small = small.filter(ImageFilter.GaussianBlur(radius=blur_amount / 10))
        background = small.resize((target_width, target_height), Image.LANCZOS)

    # Dark overlay
    overlay = Image.new("RGBA", (target_width, target_height), (0, 0, 0, 100))
    temp = background.convert("RGBA")
    final_image = Image.alpha_composite(temp, overlay).convert("RGB")

    # --- Album art (square, rounded corners) ---
    square_img = crop_to_square(Image.open(image_path).convert("RGB"))
    square_size = int(target_width * 0.7)
    padding = 10
    square_img = square_img.resize((square_size, square_size), Image.LANCZOS)

    mask = Image.new("L", (square_size, square_size), 0)
    ImageDraw.Draw(mask).rounded_rectangle(
        [(0, 0), (square_size, square_size)], 40, fill=255
    )

    x_pos = (target_width - square_size) // 2
    y_pos = int(target_height * 0.3) - 200

    # Dark background behind album art
    dark_bg = Image.new("RGBA", (square_size + padding * 2, square_size + padding * 2), (0, 0, 0, 64))
    dark_mask = Image.new("L", dark_bg.size, 0)
    ImageDraw.Draw(dark_mask).rounded_rectangle(
        [(0, 0), (dark_bg.width, dark_bg.height)], 40 + padding, fill=255
    )
    dark_bg.putalpha(dark_mask)
    final_image.paste(dark_bg.convert("RGB"), (x_pos - padding, y_pos - padding), dark_mask)

    final_image.paste(square_img, (x_pos, y_pos), mask)

    # --- Text ---
    draw = ImageDraw.Draw(final_image)
    elements_x = x_pos

    # Artist name
    artist_y = int(target_height * 0.75) - 200
    artist_font = get_font(60)
    draw.text((elements_x, artist_y), artist.upper(), fill=(255, 255, 255), font=artist_font)

    # Song title
    title_y = artist_y + 80
    title_font = get_font(45)
    draw.text((elements_x, title_y), title, fill=(255, 255, 255), font=title_font)

    # --- Progress bar ---
    progress_y = title_y + 100
    end_x = target_width - (target_width - square_size) // 2
    draw.line([(elements_x, progress_y), (end_x, progress_y)], fill=(255, 255, 255), width=5)
    dot_x = elements_x + (end_x - elements_x) * 0.3
    draw.ellipse([(dot_x - 8, progress_y - 8), (dot_x + 8, progress_y + 8)], fill=(255, 255, 255))

    # --- Media controls ---
    controls_y = progress_y + 80
    center_x = target_width // 2
    spacing = target_width // 6

    # Previous button (triangle)
    prev_x = center_x - spacing
    s = 25
    draw.polygon([(prev_x - s // 2, controls_y), (prev_x + s // 2, controls_y - s),
                  (prev_x + s // 2, controls_y + s)], fill=(255, 255, 255))

    # Pause button (circle + two bars)
    ps = 40
    draw.ellipse([(center_x - ps, controls_y - ps), (center_x + ps, controls_y + ps)],
                 outline=(255, 255, 255), width=3)
    lw, lh, sp = 6, ps, 8
    draw.rectangle([(center_x - sp - lw // 2, controls_y - lh // 2),
                    (center_x - sp + lw // 2, controls_y + lh // 2)], fill=(255, 255, 255))
    draw.rectangle([(center_x + sp - lw // 2, controls_y - lh // 2),
                    (center_x + sp + lw // 2, controls_y + lh // 2)], fill=(255, 255, 255))

    # Next button (triangle)
    next_x = center_x + spacing
    draw.polygon([(next_x + s // 2, controls_y), (next_x - s // 2, controls_y - s),
                  (next_x - s // 2, controls_y + s)], fill=(255, 255, 255))

    return final_image


def create_simple_9_16(image_path):
    """Crop and resize image to 9:16 (1080x1920) without any overlay."""
    original = Image.open(image_path).convert("RGB")
    width, height = original.size
    target_ratio = 9 / 16

    if width / height > target_ratio:
        new_w = int(height * target_ratio)
        cropped = original.crop(((width - new_w) // 2, 0, (width + new_w) // 2, height))
    else:
        new_h = int(width / target_ratio)
        cropped = original.crop((0, (height - new_h) // 2, width, (height + new_h) // 2))

    return cropped.resize((1080, 1920), Image.LANCZOS)
