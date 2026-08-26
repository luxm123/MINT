from pathlib import Path

import matplotlib.pyplot as plt
from matplotlib.patches import (
    Circle,
    Ellipse,
    FancyArrowPatch,
    FancyBboxPatch,
    Polygon,
    Rectangle,
)


ROOT = Path(__file__).resolve().parents[1]
OUT_DIR = ROOT / "figures" / "architecture"
SVG_PATH = OUT_DIR / "mint_architecture_overview.svg"
PNG_PATH = OUT_DIR / "mint_architecture_overview.png"


C = {
    "ink": "#1f2933",
    "muted": "#5f6b76",
    "line": "#2d3339",
    "offline": "#e7f1ff",
    "offline_edge": "#2f72b8",
    "online": "#e9f7ed",
    "online_edge": "#2f8b4c",
    "platform": "#f2f4f6",
    "platform_edge": "#7b858e",
    "feedback": "#f4edfb",
    "feedback_edge": "#7b4ea3",
    "intent": "#fff8e8",
    "intent_edge": "#c68a1c",
    "action": "#ffefd9",
    "action_edge": "#d77c17",
    "cold": "#ffe1e1",
    "cold_edge": "#c93434",
    "warm": "#e8f7df",
    "warm_edge": "#63a546",
    "white": "#ffffff",
}


def text(ax, x, y, s, size=8, weight="normal", color=None, ha="center", va="center", **kw):
    ax.text(
        x,
        y,
        s,
        fontsize=size,
        fontweight=weight,
        color=color or C["ink"],
        ha=ha,
        va=va,
        fontfamily="DejaVu Sans",
        **kw,
    )


def box(ax, x, y, w, h, label=None, fc=C["white"], ec=C["line"], lw=1.0, r=0.01, z=1):
    patch = FancyBboxPatch(
        (x, y),
        w,
        h,
        boxstyle=f"round,pad=0.004,rounding_size={r}",
        linewidth=lw,
        edgecolor=ec,
        facecolor=fc,
        zorder=z,
    )
    ax.add_patch(patch)
    if label:
        text(ax, x + w / 2, y + h / 2, label, size=8, weight="semibold", zorder=z + 1)
    return patch


def rect(ax, x, y, w, h, fc=C["white"], ec=C["line"], lw=1.0, z=1):
    patch = Rectangle((x, y), w, h, linewidth=lw, edgecolor=ec, facecolor=fc, zorder=z)
    ax.add_patch(patch)
    return patch


def arrow(ax, xy1, xy2, label=None, color=None, dashed=False, rad=0, lw=1.2, ms=10, tpos=0.5, dx=0, dy=0):
    color = color or C["line"]
    patch = FancyArrowPatch(
        xy1,
        xy2,
        arrowstyle="-|>",
        mutation_scale=ms,
        linewidth=lw,
        color=color,
        linestyle=(0, (4, 3)) if dashed else "solid",
        connectionstyle=f"arc3,rad={rad}",
        shrinkA=2,
        shrinkB=2,
        zorder=10,
    )
    ax.add_patch(patch)
    if label:
        x = xy1[0] + (xy2[0] - xy1[0]) * tpos + dx
        y = xy1[1] + (xy2[1] - xy1[1]) * tpos + dy
        text(
            ax,
            x,
            y,
            label,
            size=7.2,
            color=C["ink"],
            bbox=dict(boxstyle="round,pad=0.12", facecolor="white", edgecolor="none", alpha=0.94),
            zorder=12,
        )


def step(ax, x, y, n):
    ax.add_patch(Circle((x, y), 0.012, facecolor="#fff5dd", edgecolor=C["intent_edge"], linewidth=1.0, zorder=20))
    text(ax, x, y, str(n), size=6.6, weight="semibold", color=C["ink"], zorder=21)


def user_icon(ax, x, y, scale=1.0):
    ax.add_patch(Circle((x, y + 0.021 * scale), 0.010 * scale, facecolor="#ffffff", edgecolor=C["line"], linewidth=1.0, zorder=5))
    ax.add_patch(FancyBboxPatch(
        (x - 0.018 * scale, y - 0.010 * scale),
        0.036 * scale,
        0.025 * scale,
        boxstyle="round,pad=0.002,rounding_size=0.010",
        facecolor="#ffffff",
        edgecolor=C["line"],
        linewidth=1.0,
        zorder=5,
    ))


def server_icon(ax, x, y, w=0.055, h=0.050):
    for i in range(2):
        box(ax, x, y + i * h * 0.45, w, h * 0.38, fc="#ffffff", ec=C["line"], lw=1.0, r=0.004, z=5)
        ax.add_patch(Circle((x + w * 0.18, y + i * h * 0.45 + h * 0.19), h * 0.045, facecolor=C["line"], edgecolor=C["line"], zorder=6))


def cylinder(ax, x, y, w, h, fc, ec, label, sub=None):
    rect(ax, x, y + h * 0.12, w, h * 0.76, fc=fc, ec=ec, lw=1.0)
    ax.add_patch(Ellipse((x + w / 2, y + h * 0.88), w, h * 0.24, facecolor=fc, edgecolor=ec, linewidth=1.0, zorder=3))
    ax.add_patch(Ellipse((x + w / 2, y + h * 0.12), w, h * 0.24, facecolor=fc, edgecolor=ec, linewidth=1.0, zorder=3))
    text(ax, x + w / 2, y + h * 0.58, label, size=8.0, weight="semibold", zorder=4)
    if sub:
        text(ax, x + w / 2, y + h * 0.33, sub, size=6.3, color=C["muted"], zorder=4)


def mini_dag(ax, x, y, s=0.024):
    nodes = [(x, y + s), (x + s * 1.5, y + s * 1.8), (x + s * 1.5, y + s * 0.2), (x + s * 3, y + s)]
    for a, b in [(0, 1), (0, 2), (1, 3), (2, 3)]:
        arrow(ax, nodes[a], nodes[b], color="#555", lw=0.8, ms=6)
    for i, (nx, ny) in enumerate(nodes):
        ax.add_patch(Circle((nx, ny), s * 0.28, facecolor="#ffffff", edgecolor="#444", linewidth=0.9, zorder=6))
        text(ax, nx, ny, str(i + 1), size=5.2, zorder=7)


def mini_profile(ax, x, y, w=0.085, h=0.046):
    rect(ax, x, y, w, h, fc="#ffffff", ec="#8d99a3", lw=0.9)
    xs = [x + 0.012, x + 0.028, x + 0.045, x + 0.061, x + 0.074]
    ys = [y + 0.012, y + 0.025, y + 0.034, y + 0.025, y + 0.013]
    ax.plot(xs, ys, color=C["offline_edge"], linewidth=1.0, zorder=5)
    ax.plot([x + 0.012, x + w - 0.010], [y + 0.011, y + 0.011], color="#888", linewidth=0.7, zorder=5)


def lambda_tile(ax, x, y, w, h, label, state="warm"):
    if state == "warm":
        fc, ec = C["warm"], C["warm_edge"]
    elif state == "cold":
        fc, ec = C["cold"], C["cold_edge"]
    else:
        fc, ec = "#ffffff", "#8b949e"
    box(ax, x, y, w, h, fc=fc, ec=ec, lw=0.9, r=0.006)
    text(ax, x + w * 0.18, y + h * 0.55, r"$\lambda$", size=13, weight="semibold", color="#e67e22")
    text(ax, x + w * 0.60, y + h * 0.56, label, size=6.2, weight="semibold")
    if state == "cold":
        text(ax, x + w * 0.60, y + h * 0.28, "cold start", size=5.5, color=C["cold_edge"])
    elif state == "warm":
        text(ax, x + w * 0.60, y + h * 0.28, "warm", size=5.5, color=C["warm_edge"])


def action_pill(ax, x, y, w, label, emphasize=False):
    fc = C["action"] if emphasize else "#ffffff"
    ec = C["action_edge"] if emphasize else C["online_edge"]
    box(ax, x, y, w, 0.033, fc=fc, ec=ec, lw=1.0, r=0.014, z=6)
    text(ax, x + w / 2, y + 0.0165, label, size=7.4, weight="semibold", zorder=7)


def draw():
    plt.rcParams.update({"font.family": "DejaVu Sans", "svg.fonttype": "none", "pdf.fonttype": 42})
    fig, ax = plt.subplots(figsize=(12.2, 7.0), dpi=300)
    fig.patch.set_facecolor("white")
    ax.set_facecolor("white")
    ax.set_xlim(0, 1)
    ax.set_ylim(0, 1)
    ax.axis("off")

    # Input band.
    box(ax, 0.045, 0.820, 0.910, 0.120, fc="#ffffff", ec="#a8b1ba", lw=0.9, r=0.014)
    text(ax, 0.065, 0.918, "Workflow Layer / Inputs", size=10, weight="semibold", ha="left")
    box(ax, 0.090, 0.840, 0.170, 0.055, fc="#fdfdfd", ec="#a8b1ba", lw=0.9)
    mini_dag(ax, 0.112, 0.855, s=0.018)
    text(ax, 0.202, 0.872, "Workflow DAGs", size=7.5, weight="semibold")
    text(ax, 0.202, 0.854, "chain / fanout / branch", size=6.2, color=C["muted"])

    box(ax, 0.290, 0.840, 0.180, 0.055, fc="#fdfdfd", ec="#a8b1ba", lw=0.9)
    mini_profile(ax, 0.307, 0.846)
    text(ax, 0.405, 0.872, "Function profiles", size=7.5, weight="semibold")
    text(ax, 0.405, 0.853, "time, cold start, p(branch)", size=6.2, color=C["muted"])

    box(ax, 0.510, 0.840, 0.180, 0.055, fc="#fdfdfd", ec="#a8b1ba", lw=0.9)
    text(ax, 0.600, 0.873, "Platform parameters", size=7.5, weight="semibold")
    text(ax, 0.600, 0.853, "T_ret, concurrency limit", size=6.2, color=C["muted"])

    cylinder(ax, 0.750, 0.835, 0.115, 0.065, fc="#fff8e8", ec=C["intent_edge"], label="Budget", sub="B")

    # MINT system: a richer control-plane drawing.
    box(ax, 0.045, 0.330, 0.910, 0.440, fc="#fbfdff", ec="#9faab3", lw=0.95, r=0.014)
    text(ax, 0.065, 0.743, "MINT System", size=10.8, weight="semibold", ha="left")

    # Offline planner.
    box(ax, 0.075, 0.390, 0.315, 0.295, fc=C["offline"], ec=C["offline_edge"], lw=1.15, r=0.012)
    text(ax, 0.095, 0.660, "Offline Warmup Intent Planner", size=9.4, weight="semibold", ha="left")

    box(ax, 0.098, 0.585, 0.115, 0.052, fc="#ffffff", ec="#75a7d7", lw=0.9)
    mini_dag(ax, 0.115, 0.596, s=0.018)
    text(ax, 0.178, 0.611, "DAG model", size=6.6, weight="semibold")

    box(ax, 0.238, 0.585, 0.118, 0.052, fc="#ffffff", ec="#75a7d7", lw=0.9)
    mini_profile(ax, 0.252, 0.591, w=0.060, h=0.038)
    text(ax, 0.326, 0.611, "profiles", size=6.6, weight="semibold")

    box(ax, 0.098, 0.505, 0.258, 0.052, fc="#ffffff", ec="#75a7d7", lw=0.9)
    text(ax, 0.130, 0.531, "hot/cold state model", size=6.8, weight="semibold", ha="left")
    text(ax, 0.330, 0.531, "T_ret", size=6.8, weight="semibold", color=C["offline_edge"])
    ax.plot([0.215, 0.305], [0.516, 0.516], color=C["offline_edge"], linewidth=1.0)
    ax.plot([0.215, 0.215], [0.510, 0.522], color=C["offline_edge"], linewidth=1.0)
    ax.plot([0.305, 0.305], [0.510, 0.522], color=C["offline_edge"], linewidth=1.0)

    box(ax, 0.098, 0.425, 0.258, 0.052, fc="#ffffff", ec="#75a7d7", lw=0.9)
    text(ax, 0.227, 0.456, "Benefit-aware intent planning", size=7.1, weight="semibold")
    text(ax, 0.227, 0.436, "expected benefit + branch probability under B", size=6.0, color=C["muted"])

    arrow(ax, (0.157, 0.585), (0.157, 0.558), color=C["offline_edge"], lw=0.9, ms=7)
    arrow(ax, (0.297, 0.585), (0.297, 0.558), color=C["offline_edge"], lw=0.9, ms=7)
    arrow(ax, (0.227, 0.505), (0.227, 0.478), color=C["offline_edge"], lw=0.9, ms=7)

    # Intent repository between offline and runtime.
    cylinder(ax, 0.420, 0.495, 0.140, 0.130, fc=C["intent"], ec=C["intent_edge"], label="Warmup\nIntent Store", sub="target, time, benefit")
    for i in range(3):
        rect(ax, 0.468 + i * 0.014, 0.498 + i * 0.014, 0.044, 0.018, fc="#ffffff", ec=C["intent_edge"], lw=0.7, z=5)

    # Runtime orchestrator.
    box(ax, 0.600, 0.390, 0.325, 0.295, fc=C["online"], ec=C["online_edge"], lw=1.15, r=0.012)
    text(ax, 0.620, 0.660, "Runtime-adaptive Orchestrator", size=9.4, weight="semibold", ha="left")
    user_icon(ax, 0.895, 0.650, scale=0.82)
    for i in range(3):
        rect(ax, 0.848 + i * 0.012, 0.663, 0.008, 0.008, fc="#8fc3ef", ec="#4c83b6", lw=0.7, z=5)
    arrow(ax, (0.888, 0.652), (0.835, 0.628), color=C["line"], lw=0.9, ms=7, tpos=0.40, dy=0.022)
    text(ax, 0.905, 0.625, "requests", size=6.3, color=C["muted"])

    box(ax, 0.625, 0.590, 0.115, 0.048, fc="#ffffff", ec="#78ad84", lw=0.9)
    text(ax, 0.682, 0.617, "State Tracker", size=6.8, weight="semibold")
    text(ax, 0.682, 0.600, "state + progress", size=5.7, color=C["muted"])

    box(ax, 0.775, 0.590, 0.115, 0.048, fc="#ffffff", ec="#78ad84", lw=0.9)
    text(ax, 0.832, 0.617, "Re-evaluator", size=6.8, weight="semibold")
    text(ax, 0.832, 0.600, "benefit, validity", size=5.7, color=C["muted"])

    box(ax, 0.625, 0.505, 0.265, 0.055, fc="#ffffff", ec="#78ad84", lw=0.9)
    text(ax, 0.758, 0.541, "Adaptive Action Selector", size=7.3, weight="semibold")
    x0 = 0.642
    for label, emph in [("Execute", False), ("Cancel", False), ("Replace", True), ("Delay", True)]:
        action_pill(ax, x0, 0.510, 0.055, label, emphasize=emph)
        x0 += 0.061

    box(ax, 0.675, 0.425, 0.165, 0.048, fc="#ffffff", ec="#78ad84", lw=0.9)
    text(ax, 0.758, 0.452, "Warmup Executor", size=7.0, weight="semibold")
    text(ax, 0.758, 0.435, "async calls under B", size=5.8, color=C["muted"])

    arrow(ax, (0.740, 0.614), (0.775, 0.614), color=C["online_edge"], lw=1.0, ms=8)
    arrow(ax, (0.832, 0.590), (0.805, 0.560), color=C["online_edge"], lw=1.0, ms=8)
    arrow(ax, (0.758, 0.505), (0.758, 0.473), color=C["online_edge"], lw=1.0, ms=8)

    # Main arrows through MINT.
    arrow(ax, (0.505, 0.820), (0.230, 0.685), "workflow, profiles, platform, B", color=C["line"], lw=1.0, ms=9, tpos=0.50, dy=0.025)
    arrow(ax, (0.390, 0.535), (0.420, 0.555), color=C["offline_edge"], lw=1.2, ms=10)
    arrow(ax, (0.560, 0.555), (0.600, 0.555), "Warmup intents", color=C["intent_edge"], lw=1.2, ms=10, tpos=0.50, dy=0.035)
    step(ax, 0.435, 0.583, 1)
    step(ax, 0.581, 0.574, 2)
    step(ax, 0.846, 0.642, 3)

    # Serverless platform with visual runtime elements.
    box(ax, 0.045, 0.155, 0.910, 0.130, fc=C["platform"], ec=C["platform_edge"], lw=0.95, r=0.014)
    text(ax, 0.065, 0.260, "Serverless Platform", size=10, weight="semibold", ha="left")
    box(ax, 0.090, 0.175, 0.260, 0.070, fc="#ffffff", ec="#8d969d", lw=0.9)
    text(ax, 0.105, 0.232, "AWS Lambda functions", size=7.4, weight="semibold", ha="left")
    lambda_tile(ax, 0.110, 0.185, 0.066, 0.035, "f1", "warm")
    lambda_tile(ax, 0.190, 0.185, 0.066, 0.035, "f2", "cold")
    lambda_tile(ax, 0.270, 0.185, 0.066, 0.035, "f3", "warm")

    box(ax, 0.390, 0.175, 0.185, 0.070, fc="#ffffff", ec="#8d969d", lw=0.9)
    text(ax, 0.482, 0.229, "instance retention", size=7.2, weight="semibold")
    ax.plot([0.420, 0.548], [0.200, 0.200], color=C["platform_edge"], linewidth=1.2)
    ax.plot([0.430, 0.430], [0.194, 0.206], color=C["platform_edge"], linewidth=1.2)
    ax.plot([0.535, 0.535], [0.194, 0.206], color=C["platform_edge"], linewidth=1.2)
    text(ax, 0.482, 0.188, "T_ret", size=6.2, color=C["muted"])

    box(ax, 0.615, 0.175, 0.125, 0.070, fc="#ffffff", ec="#8d969d", lw=0.9)
    text(ax, 0.678, 0.229, "cold-start", size=7.2, weight="semibold")
    ax.add_patch(Polygon([(0.666, 0.192), (0.690, 0.192), (0.678, 0.220)], closed=True, facecolor=C["cold"], edgecolor=C["cold_edge"], linewidth=1.0))
    text(ax, 0.678, 0.186, "event", size=5.8, color=C["muted"])

    box(ax, 0.785, 0.175, 0.120, 0.070, fc="#ffffff", ec="#8d969d", lw=0.9)
    text(ax, 0.845, 0.224, "concurrency", size=7.2, weight="semibold")
    text(ax, 0.845, 0.200, "limit", size=7.2, weight="semibold")

    arrow(ax, (0.715, 0.425), (0.235, 0.245), "Workflow invocation", color=C["online_edge"], lw=1.2, ms=10, tpos=0.55, dy=0.025)
    arrow(ax, (0.805, 0.425), (0.677, 0.245), "Asynchronous warmup invocation", color=C["online_edge"], lw=1.2, ms=10, tpos=0.48, dx=0.050, dy=-0.030)
    step(ax, 0.540, 0.345, 4)
    step(ax, 0.735, 0.325, 5)

    # Observability and feedback as a pipeline.
    box(ax, 0.045, 0.035, 0.910, 0.085, fc=C["feedback"], ec=C["feedback_edge"], lw=0.95, r=0.014)
    text(ax, 0.065, 0.096, "Observability & Feedback", size=9.6, weight="semibold", ha="left")
    fb_x = [0.225, 0.405, 0.585, 0.765]
    fb_labels = ["Metrics\nCollection", "Hot/cold\nInference", "Profile\nCalibration", "Performance\nEvaluation"]
    for x, label in zip(fb_x, fb_labels):
        box(ax, x, 0.052, 0.120, 0.036, fc="#ffffff", ec="#9670b0", lw=0.9)
        text(ax, x + 0.060, 0.070, label, size=6.6, weight="semibold")
    for x in [0.345, 0.525, 0.705]:
        arrow(ax, (x, 0.070), (x + 0.060, 0.070), color=C["feedback_edge"], lw=0.9, ms=7)

    arrow(ax, (0.500, 0.155), (0.500, 0.120), "Latency, cold-start events, warmup outcomes", color=C["platform_edge"], lw=1.0, ms=8, dy=0.001)
    arrow(ax, (0.610, 0.088), (0.220, 0.390), "Profile update", color=C["feedback_edge"], dashed=True, rad=-0.24, lw=1.1, ms=9, tpos=0.30, dx=-0.105, dy=0.085)
    arrow(ax, (0.820, 0.088), (0.855, 0.425), "Runtime feedback", color=C["feedback_edge"], dashed=True, rad=0.20, lw=1.1, ms=9, tpos=0.68, dx=0.070, dy=0.025)
    step(ax, 0.515, 0.136, 6)

    # Small legend.
    box(ax, 0.710, 0.720, 0.055, 0.028, "Offline", fc=C["offline"], ec=C["offline_edge"], lw=0.8, r=0.012)
    box(ax, 0.775, 0.720, 0.055, 0.028, "Online", fc=C["online"], ec=C["online_edge"], lw=0.8, r=0.012)
    box(ax, 0.840, 0.720, 0.065, 0.028, "Feedback", fc=C["feedback"], ec=C["feedback_edge"], lw=0.8, r=0.012)

    fig.savefig(SVG_PATH, bbox_inches="tight", facecolor="white")
    fig.savefig(PNG_PATH, bbox_inches="tight", facecolor="white", dpi=300)
    plt.close(fig)


if __name__ == "__main__":
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    draw()
    print(f"Wrote {SVG_PATH}")
    print(f"Wrote {PNG_PATH}")
