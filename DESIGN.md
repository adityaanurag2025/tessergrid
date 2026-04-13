# Tessergrid — Design System Specification

## 1. Overview & Creative North Star
### The Creative North Star: "The Pristine Ledger"
This design system moves away from the cluttered, grid-heavy aesthetic of traditional data tools. Instead, it adopts the persona of **The Pristine Ledger**—an editorial-grade environment where data is treated with the reverence of high-end journalism. 

The system breaks the "template" look by utilizing **intentional asymmetry** and **tonal depth**. Rather than boxing data into rigid containers, we use expansive breathing room (whitespace) and sophisticated layering to guide the user’s eye. The goal is to transform the chore of data cleaning into an experience of clarity, where the interface feels less like a spreadsheet and more like a high-performance architectural plan.

---

## 2. Colors
Our palette relies on a sophisticated interplay of deep slate neutrals and high-clarity accents. The color strategy is designed to minimize cognitive load while highlighting the "purity" of cleaned data.

### The "No-Line" Rule
**Strict Mandate:** Designers are prohibited from using 1px solid borders to define sections or cards. 
Structure must be achieved through:
*   **Tonal Shifts:** Placing a `surface-container-low` component against a `surface` background.
*   **Negative Space:** Using the Spacing Scale to create boundaries.
*   **Subtle Depth:** Using high-contrast layering between `surface-container-lowest` and `surface-container-highest`.

### Surface Hierarchy & Nesting
Treat the UI as a series of physical layers. 
*   **Base:** `surface` (#f7f9fd)
*   **Primary Containers:** `surface-container-low` (#f2f4f8) for secondary sidebars or background groupings.
*   **Active Workspaces:** `surface-container-lowest` (#ffffff) for the main data table or primary content area, creating a "lifted" focal point.

### The "Glass & Gradient" Rule
To inject visual "soul" into the tool, use **Glassmorphism** for floating elements (e.g., file upload overlays, tooltips). Apply a `backdrop-blur` of 12px-20px with a semi-transparent `surface-container` color.
*   **CTAs:** Use a subtle linear gradient from `primary` (#000000) to `primary-container` (#131b2e) to provide a premium, weighted feel to action buttons.
*   **Success States:** Use `tertiary` (#000000) and its container variants to highlight "Cleaned" data rows, providing a clear visual reward.

---

## 3. Typography
We utilize the **Inter** family to provide a modern, technical, yet highly readable foundation. 

*   **Display & Headlines:** Used for "Data Summaries" and "Success Milestones." Use `display-md` with slightly tighter letter-spacing (-0.02em) to create an authoritative, editorial presence.
*   **Data Tables:** Use `body-md` for row content. The clarity of the "Inter" typeface ensures that even complex alphanumeric strings remain legible.
*   **Labels:** `label-sm` should be used for metadata and table headers. To achieve a premium feel, apply **uppercase casing** with **+0.05em letter spacing**. This differentiates "data" from "instruction."

Hierarchy is not just about size; it is about weight and tone. Headlines should feel like a statement, while labels should feel like a functional whisper.

---

## 4. Elevation & Depth
In this design system, depth is a function of light and layering, not artificial lines.

### The Layering Principle
Achieve hierarchy by "stacking" surface tiers. Place a `surface-container-lowest` card on top of a `surface-container-low` background to create a soft, natural lift. This mimics the way fine paper sits on a desk.

### Ambient Shadows
For floating elements (Modals, Dropdowns), use **Ambient Shadows**. 
*   **Properties:** Blur: 24px-40px | Opacity: 4%-6% | Color: Derived from `on-surface` (#191c1f).
*   Avoid the "dirty" look of standard grey drop shadows; the shadow should feel like a soft glow of occlusion.

### The "Ghost Border" Fallback
If a border is absolutely required for accessibility (e.g., high-contrast mode or input focus), use a **Ghost Border**. 
*   **Token:** `outline-variant` (#c6c6cd) at **15% opacity**.
*   **Never** use 100% opaque borders.

---

## 5. Components

### File Upload (The "Glass" Dropzone)
The file upload component should not be a dashed box. Instead, use a `surface-container-low` area with a `surface-variant` hover state. On "drag-over," apply a glassmorphic blur and a `primary` subtle gradient border.

### Data Tables (The "Pristine Grid")
*   **Borders:** Forbidden. 
*   **Separation:** Use alternating row fills of `surface-container-low` or simply use generous vertical padding (`body-md` with 16px top/bottom) to let the white space act as the divider.
*   **Headers:** Use `label-md` in `on-surface-variant` with a background of `surface-container-highest` for clear categorization.

### Buttons
*   **Primary:** `primary` background with `on-primary` text. Use a `xl` (0.75rem) roundedness for a modern, approachable feel.
*   **Secondary:** No background. Use a `ghost-border` on hover with `on-surface` text.
*   **Tertiary (Success Action):** Use the `tertiary_fixed` (#6ffbbe) color for "Clean Data" buttons to make them pop against the slate/blue environment.

### Inputs & Fields
Inputs should use `surface-container-highest` as a background. Instead of a 4-sided border, use a 2px bottom-accent in `primary` that only appears on `:focus`. This maintains the "Editorial" look while providing clear interaction feedback.

---

## 6. Do's and Don'ts

### Do:
*   **Do** embrace asymmetry. If a data summary can sit off-center to create a more dynamic layout, let it.
*   **Do** use `tertiary` (#000000/Emerald) sparingly to highlight "Value Added" or "Cleaned" states.
*   **Do** maximize whitespace. If it feels like there is too much room, add 8px more.

### Don't:
*   **Don't** use 1px solid dividers to separate list items. Use vertical spacing instead.
*   **Don't** use pure black for text on a white background; use `on-surface` (#191c1f) to reduce eye strain.
*   **Don't** use traditional "Material" style drop shadows. Stick to Tonal Layering or Ambient Shadows.
*   **Don't** use high-contrast outlines for checkboxes or radio buttons; use tonal fills (`surface-container-highest`) to indicate the "off" state.