use eframe::egui;
use xai_grok_operator_core::theme::{
    self as shared, GOLD, GOLD_MATTE, GREEN_TEXT, HOT_PINK, NEON_GREEN, OBSIDIAN, ON_GOLD,
    ON_GREEN, ON_SURFACE, ON_SURFACE_VARIANT, OUTLINE, OUTLINE_VARIANT, PINK_TEXT, Rgb, SURFACE,
    SURFACE_HIGH, SURFACE_HIGHEST, SURFACE_LOW, SemanticTone,
};

pub const GUTTER: i8 = 16;

pub fn color(rgb: Rgb) -> egui::Color32 {
    let (red, green, blue) = rgb.tuple();
    egui::Color32::from_rgb(red, green, blue)
}

pub fn tone_color(tone: SemanticTone) -> egui::Color32 {
    color(shared::tone_rgb(tone))
}

pub fn configure(context: &egui::Context) {
    context.set_theme(egui::Theme::Dark);
    let mut style = (*context.style_of(egui::Theme::Dark)).clone();
    style.animation_time = 0.12;
    style.spacing.item_spacing = egui::vec2(8.0, 8.0);
    style.spacing.button_padding = egui::vec2(12.0, 7.0);
    style.spacing.indent = 16.0;
    style.spacing.interact_size = egui::vec2(44.0, 32.0);
    style.text_styles.insert(
        egui::TextStyle::Heading,
        egui::FontId::new(26.0, egui::FontFamily::Proportional),
    );
    style.text_styles.insert(
        egui::TextStyle::Body,
        egui::FontId::new(16.0, egui::FontFamily::Proportional),
    );
    style.text_styles.insert(
        egui::TextStyle::Button,
        egui::FontId::new(14.0, egui::FontFamily::Monospace),
    );
    style.text_styles.insert(
        egui::TextStyle::Monospace,
        egui::FontId::new(13.0, egui::FontFamily::Monospace),
    );
    style.text_styles.insert(
        egui::TextStyle::Small,
        egui::FontId::new(12.0, egui::FontFamily::Monospace),
    );

    let mut visuals = egui::Visuals::dark();
    visuals.override_text_color = Some(color(ON_SURFACE));
    visuals.weak_text_color = Some(color(OUTLINE));
    visuals.panel_fill = color(OBSIDIAN);
    visuals.window_fill = color(SURFACE_LOW);
    visuals.window_stroke = egui::Stroke::new(3.0, color(GOLD));
    visuals.window_corner_radius = egui::CornerRadius::ZERO;
    visuals.window_shadow = egui::Shadow {
        offset: [8, 8],
        blur: 0,
        spread: 0,
        color: color(GOLD_MATTE).gamma_multiply(0.45),
    };
    visuals.menu_corner_radius = egui::CornerRadius::ZERO;
    visuals.popup_shadow = egui::Shadow {
        offset: [5, 5],
        blur: 0,
        spread: 0,
        color: color(HOT_PINK).gamma_multiply(0.35),
    };
    visuals.extreme_bg_color = color(OBSIDIAN);
    visuals.text_edit_bg_color = Some(color(SURFACE));
    visuals.code_bg_color = color(SURFACE);
    visuals.faint_bg_color = color(SURFACE_LOW);
    visuals.hyperlink_color = color(GREEN_TEXT);
    visuals.warn_fg_color = color(PINK_TEXT);
    visuals.error_fg_color = color(shared::ERROR);
    visuals.selection.bg_fill = color(NEON_GREEN);
    visuals.selection.stroke = egui::Stroke::new(2.0, color(ON_GREEN));
    visuals.button_frame = true;
    visuals.collapsing_header_frame = true;
    visuals.indent_has_left_vline = true;
    visuals.striped = false;

    let square = egui::CornerRadius::ZERO;
    visuals.widgets.noninteractive.bg_fill = color(SURFACE_LOW);
    visuals.widgets.noninteractive.weak_bg_fill = color(SURFACE);
    visuals.widgets.noninteractive.bg_stroke = egui::Stroke::new(1.0, color(OUTLINE_VARIANT));
    visuals.widgets.noninteractive.fg_stroke = egui::Stroke::new(1.0, color(ON_SURFACE_VARIANT));
    visuals.widgets.noninteractive.corner_radius = square;

    visuals.widgets.inactive.bg_fill = color(SURFACE_HIGH);
    visuals.widgets.inactive.weak_bg_fill = color(SURFACE_LOW);
    visuals.widgets.inactive.bg_stroke = egui::Stroke::new(1.0, color(GOLD_MATTE));
    visuals.widgets.inactive.fg_stroke = egui::Stroke::new(1.0, color(GOLD));
    visuals.widgets.inactive.corner_radius = square;

    visuals.widgets.hovered.bg_fill = color(NEON_GREEN);
    visuals.widgets.hovered.weak_bg_fill = color(SURFACE_HIGHEST);
    visuals.widgets.hovered.bg_stroke = egui::Stroke::new(2.0, color(HOT_PINK));
    visuals.widgets.hovered.fg_stroke = egui::Stroke::new(2.0, color(ON_GREEN));
    visuals.widgets.hovered.corner_radius = square;
    visuals.widgets.hovered.expansion = 0.0;

    visuals.widgets.active.bg_fill = color(GOLD);
    visuals.widgets.active.weak_bg_fill = color(GOLD_MATTE);
    visuals.widgets.active.bg_stroke = egui::Stroke::new(2.0, color(NEON_GREEN));
    visuals.widgets.active.fg_stroke = egui::Stroke::new(2.0, color(ON_GOLD));
    visuals.widgets.active.corner_radius = square;
    visuals.widgets.active.expansion = 0.0;

    visuals.widgets.open = visuals.widgets.active;
    style.visuals = visuals;
    context.set_style_of(egui::Theme::Dark, style.clone());
    context.set_style_of(egui::Theme::Light, style.clone());
    context.set_global_style(style);
}

pub fn panel_frame(fill: Rgb) -> egui::Frame {
    egui::Frame::new()
        .inner_margin(GUTTER)
        .fill(color(fill))
        .stroke(egui::Stroke::new(1.0, color(OUTLINE_VARIANT)))
        .corner_radius(egui::CornerRadius::ZERO)
}

pub fn deco_frame(fill: Rgb, tone: SemanticTone) -> egui::Frame {
    let accent = tone_color(tone);
    egui::Frame::new()
        .inner_margin(12)
        .outer_margin(4)
        .fill(color(fill))
        .stroke(egui::Stroke::new(2.0, accent))
        .corner_radius(egui::CornerRadius::ZERO)
        .shadow(egui::Shadow {
            offset: [4, 4],
            blur: 0,
            spread: 0,
            color: accent.gamma_multiply(0.24),
        })
}

pub fn draw_backdrop(ui: &egui::Ui) {
    let rect = ui.max_rect();
    let painter = ui.painter();
    painter.rect_filled(rect, 0.0, color(OBSIDIAN));

    let fine = color(OUTLINE_VARIANT).gamma_multiply(0.18);
    let coarse = color(GOLD_MATTE).gamma_multiply(0.10);
    let mut x = rect.left();
    let mut column = 0_u32;
    while x <= rect.right() {
        let stroke = if column.is_multiple_of(4) {
            egui::Stroke::new(1.0, coarse)
        } else {
            egui::Stroke::new(1.0, fine)
        };
        painter.line_segment(
            [egui::pos2(x, rect.top()), egui::pos2(x, rect.bottom())],
            stroke,
        );
        x += 20.0;
        column += 1;
    }
    let mut y = rect.top();
    let mut row = 0_u32;
    while y <= rect.bottom() {
        let stroke = if row.is_multiple_of(4) {
            egui::Stroke::new(1.0, coarse)
        } else {
            egui::Stroke::new(1.0, fine)
        };
        painter.line_segment(
            [egui::pos2(rect.left(), y), egui::pos2(rect.right(), y)],
            stroke,
        );
        y += 20.0;
        row += 1;
    }
}

pub fn section_heading(ui: &mut egui::Ui, kicker: &str, title: impl Into<String>) {
    ui.label(
        egui::RichText::new(kicker.to_ascii_uppercase())
            .monospace()
            .size(11.0)
            .color(color(GREEN_TEXT))
            .strong(),
    );
    ui.label(
        egui::RichText::new(title.into().to_ascii_uppercase())
            .size(24.0)
            .strong()
            .italics()
            .color(color(GOLD)),
    );
    let (rect, _) =
        ui.allocate_exact_size(egui::vec2(ui.available_width(), 5.0), egui::Sense::hover());
    ui.painter().line_segment(
        [
            rect.left_center(),
            egui::pos2(rect.right(), rect.center().y),
        ],
        egui::Stroke::new(2.0, color(GOLD_MATTE)),
    );
    let center = rect.center();
    ui.painter().line_segment(
        [egui::pos2(center.x - 14.0, rect.top()), center],
        egui::Stroke::new(2.0, color(HOT_PINK)),
    );
    ui.painter().line_segment(
        [center, egui::pos2(center.x + 14.0, rect.bottom())],
        egui::Stroke::new(2.0, color(NEON_GREEN)),
    );
}

pub fn status_chip(ui: &mut egui::Ui, label: impl Into<String>, tone: SemanticTone) {
    let accent = tone_color(tone);
    let text = match tone {
        SemanticTone::Live => color(ON_GREEN),
        SemanticTone::Primary => color(ON_GOLD),
        _ => accent,
    };
    let fill = match tone {
        SemanticTone::Live => color(NEON_GREEN),
        SemanticTone::Primary => color(GOLD),
        _ => color(SURFACE),
    };
    egui::Frame::new()
        .inner_margin(egui::Margin::symmetric(7, 3))
        .fill(fill)
        .stroke(egui::Stroke::new(1.0, accent))
        .corner_radius(egui::CornerRadius::ZERO)
        .show(ui, |ui| {
            ui.label(
                egui::RichText::new(label.into().to_ascii_uppercase())
                    .monospace()
                    .size(11.0)
                    .strong()
                    .color(text),
            );
        });
}

pub fn action_button(ui: &mut egui::Ui, label: &str, enabled: bool) -> egui::Response {
    let desired = egui::vec2((label.len() as f32 * 9.0 + 36.0).max(132.0), 38.0);
    let sense = if enabled {
        egui::Sense::click()
    } else {
        egui::Sense::hover()
    };
    let (rect, response) = ui.allocate_exact_size(desired, sense);
    let hovered = response.hovered() && enabled;
    let pressed = response.is_pointer_button_down_on() && enabled;
    let chamfer = 8.0;
    let points = vec![
        egui::pos2(rect.left() + chamfer, rect.top()),
        egui::pos2(rect.right() - chamfer, rect.top()),
        egui::pos2(rect.right(), rect.top() + chamfer),
        egui::pos2(rect.right(), rect.bottom() - chamfer),
        egui::pos2(rect.right() - chamfer, rect.bottom()),
        egui::pos2(rect.left() + chamfer, rect.bottom()),
        egui::pos2(rect.left(), rect.bottom() - chamfer),
        egui::pos2(rect.left(), rect.top() + chamfer),
    ];
    let fill = if !enabled {
        color(SURFACE_HIGH)
    } else if hovered {
        color(NEON_GREEN)
    } else {
        color(GOLD)
    };
    let foreground = if !enabled {
        color(OUTLINE)
    } else if hovered {
        color(ON_GREEN)
    } else {
        color(ON_GOLD)
    };
    if enabled {
        let offset = if pressed { 1.0 } else { 5.0 };
        let shadow = points
            .iter()
            .map(|point| *point + egui::vec2(offset, offset))
            .collect();
        ui.painter().add(egui::Shape::convex_polygon(
            shadow,
            if hovered {
                color(HOT_PINK).gamma_multiply(0.65)
            } else {
                color(GOLD_MATTE).gamma_multiply(0.38)
            },
            egui::Stroke::NONE,
        ));
    }
    ui.painter().add(egui::Shape::convex_polygon(
        points,
        fill,
        egui::Stroke::new(
            2.0,
            if hovered {
                color(HOT_PINK)
            } else {
                color(GOLD)
            },
        ),
    ));
    ui.painter().text(
        rect.center()
            + if pressed {
                egui::vec2(1.0, 1.0)
            } else {
                egui::Vec2::ZERO
            },
        egui::Align2::CENTER_CENTER,
        label.to_ascii_uppercase(),
        egui::FontId::new(13.0, egui::FontFamily::Monospace),
        foreground,
    );
    response
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn style_uses_the_locked_square_gilded_palette() {
        let context = egui::Context::default();
        configure(&context);
        let style = context.style_of(egui::Theme::Dark);
        assert_eq!(style.visuals.panel_fill, color(OBSIDIAN));
        assert_eq!(style.visuals.window_stroke.color, color(GOLD));
        assert_eq!(
            style.visuals.widgets.inactive.corner_radius,
            egui::CornerRadius::ZERO
        );
        assert_eq!(style.visuals.widgets.hovered.bg_fill, color(NEON_GREEN));
        assert_eq!(
            style.visuals.widgets.hovered.bg_stroke.color,
            color(HOT_PINK)
        );
    }
}
