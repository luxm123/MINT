from pathlib import Path

import matplotlib.pyplot as plt
from matplotlib.patches import Arc, Circle, Ellipse, FancyArrowPatch, FancyBboxPatch, Rectangle


ROOT = Path(__file__).resolve().parents[1]
OUT_DIR = ROOT / "figures" / "architecture"
SVG_PATH = OUT_DIR / "mint_system_architecture.svg"
PNG_PATH = OUT_DIR / "mint_system_architecture.png"


C = {
    "ink": "#111827",
    "muted": "#475569",
    "line": "#111827",
    "panel": "#f8fbff",
    "top_edge": "#8da0b8",
    "blue": "#0b3b91",
    "blue_fill": "#eef6ff",
    "blue_light": "#dcebff",
    "green": "#2e8b3c",
    "green_fill": "#f0fbf1",
    "gray": "#606975",
    "gray_fill": "#f5f6f8",
    "purple": "#6b2ca0",
    "purple_fill": "#fbf5ff",
    "orange": "#9a6800",
    "orange_fill": "#fff8df",
    "warm": "#edf9e8",
    "warm_edge": "#5c9b41",
    "cold": "#fff1ef",
    "cold_edge": "#d94941",
    "white": "#ffffff",
}


def txt(ax, x, y, s, size=10, weight="normal", color=None, ha="center", va="center", **kw):
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


def box(ax, x, y, w, h, fc=C["white"], ec=C["line"], lw=1.0, r=0.10, z=1):
    patch = FancyBboxPatch(
        (x, y),
        w,
        h,
        boxstyle=f"round,pad=0.015,rounding_size={r}",
        linewidth=lw,
        edgecolor=ec,
        facecolor=fc,
        zorder=z,
    )
    ax.add_patch(patch)
    return patch


def rect(ax, x, y, w, h, fc=C["white"], ec=C["line"], lw=1.0, z=1):
    patch = Rectangle((x, y), w, h, facecolor=fc, edgecolor=ec, linewidth=lw, zorder=z)
    ax.add_patch(patch)
    return patch


def arrow(ax, a, b, color=None, dashed=False, lw=1.2, ms=12, rad=0, label=None, t=0.5, dx=0, dy=0):
    color = color or C["line"]
    patch = FancyArrowPatch(
        a,
        b,
        arrowstyle="-|>",
        mutation_scale=ms,
        linewidth=lw,
        color=color,
        linestyle=(0, (4, 4)) if dashed else "solid",
        connectionstyle=f"arc3,rad={rad}",
        shrinkA=2,
        shrinkB=2,
        zorder=20,
    )
    ax.add_patch(patch)
    if label:
        x = a[0] + (b[0] - a[0]) * t + dx
        y = a[1] + (b[1] - a[1]) * t + dy
        txt(
            ax,
            x,
            y,
            label,
            size=8.0,
            color=C["ink"],
            bbox=dict(facecolor="white", edgecolor="none", boxstyle="round,pad=0.10", alpha=0.94),
            zorder=30,
        )


def cylinder(ax, x, y, w, h, title, sub, fc=C["orange_fill"], ec=C["orange"]):
    rect(ax, x, y + 0.10 * h, w, 0.80 * h, fc=fc, ec=ec, lw=1.2, z=3)
    ax.add_patch(Ellipse((x + w / 2, y + 0.90 * h), w, 0.22 * h, facecolor=fc, edgecolor=ec, linewidth=1.2, zorder=4))
    ax.add_patch(Ellipse((x + w / 2, y + 0.10 * h), w, 0.22 * h, facecolor=fc, edgecolor=ec, linewidth=1.2, zorder=4))
    txt(ax, x + w / 2, y + 0.60 * h, title, size=13, weight="bold", zorder=5)
    txt(ax, x + w / 2, y + 0.34 * h, sub, size=9.2, color=C["muted"], zorder=5)


def icon_dag(ax, x, y, s=0.32, color="#2b5b99"):
    pts = [(x, y), (x + s, y + 0.42), (x + s, y - 0.42), (x + 2 * s, y), (x + 2.55 * s, y - 0.50)]
    for i, j, ls in [(0, 1, "-"), (0, 2, "-"), (1, 3, "-"), (2, 3, "--"), (3, 4, "-")]:
        ax.plot([pts[i][0], pts[j][0]], [pts[i][1], pts[j][1]], color="#1f2937", linewidth=0.9, linestyle=ls, zorder=6)
    for px, py in pts:
        ax.add_patch(Circle((px, py), 0.075, facecolor="#dbeafe", edgecolor="#1f2937", linewidth=0.9, zorder=7))


def icon_profile(ax, x, y):
    ax.plot([x, x], [y - 0.33, y + 0.34], color=C["line"], linewidth=1.0)
    ax.plot([x, x + 0.72], [y - 0.33, y - 0.33], color=C["line"], linewidth=1.0)
    xs = [x + 0.08, x + 0.18, x + 0.28, x + 0.40, x + 0.55, x + 0.66]
    ys = [y - 0.20, y + 0.10, y - 0.05, y - 0.18, y + 0.20, y + 0.03]
    ax.plot(xs, ys, color="#2f6fb2", linewidth=1.4)
    ax.add_patch(Circle((x + 0.58, y + 0.35), 0.14, facecolor="white", edgecolor=C["line"], linewidth=1.0))
    ax.plot([x + 0.58, x + 0.58], [y + 0.35, y + 0.44], color=C["line"], linewidth=1.0)
    ax.plot([x + 0.58, x + 0.50], [y + 0.35, y + 0.35], color=C["line"], linewidth=1.0)


def icon_gear(ax, x, y):
    ax.add_patch(Circle((x, y), 0.32, facecolor="#e5e7eb", edgecolor=C["line"], linewidth=1.0))
    ax.add_patch(Circle((x, y), 0.14, facecolor="white", edgecolor=C["line"], linewidth=1.0))
    for dx, dy in [(0, 0.42), (0, -0.42), (0.42, 0), (-0.42, 0), (0.30, 0.30), (-0.30, 0.30), (0.30, -0.30), (-0.30, -0.30)]:
        rect(ax, x + dx - 0.045, y + dy - 0.045, 0.09, 0.09, fc="#e5e7eb", ec=C["line"], lw=0.8, z=5)


def icon_budget(ax, x, y):
    rect(ax, x - 0.26, y - 0.18, 0.42, 0.34, fc="#e0f2fe", ec=C["line"], lw=1.0, z=5)
    rect(ax, x - 0.18, y - 0.11, 0.42, 0.34, fc="#bae6fd", ec=C["line"], lw=1.0, z=6)
    ax.add_patch(Circle((x + 0.25, y - 0.18), 0.14, facecolor="#fde047", edgecolor=C["line"], linewidth=1.0, zorder=7))
    txt(ax, x + 0.25, y - 0.18, "B", size=9, weight="bold", zorder=8)


def icon_user(ax, x, y):
    ax.add_patch(Circle((x, y + 0.22), 0.14, facecolor="white", edgecolor=C["line"], linewidth=1.2))
    ax.add_patch(Arc((x, y - 0.08), 0.55, 0.55, theta1=0, theta2=180, edgecolor=C["line"], linewidth=1.2))
    ax.plot([x - 0.275, x - 0.275], [y - 0.08, y - 0.35], color=C["line"], linewidth=1.2)
    ax.plot([x + 0.275, x + 0.275], [y - 0.08, y - 0.35], color=C["line"], linewidth=1.2)
    ax.plot([x - 0.275, x + 0.275], [y - 0.35, y - 0.35], color=C["line"], linewidth=1.2)


def icon_clock(ax, x, y):
    ax.add_patch(Circle((x, y), 0.27, facecolor="#e0f2fe", edgecolor=C["blue"], linewidth=1.2))
    ax.add_patch(Circle((x, y), 0.19, facecolor="white", edgecolor=C["blue"], linewidth=1.2))
    ax.plot([x, x], [y, y + 0.12], color=C["blue"], linewidth=1.4)
    ax.plot([x, x + 0.10], [y, y], color=C["blue"], linewidth=1.4)


def icon_target(ax, x, y):
    for r in [0.30, 0.20, 0.10]:
        ax.add_patch(Circle((x, y), r, facecolor="none", edgecolor=C["blue"], linewidth=1.2))
    arrow(ax, (x + 0.04, y + 0.04), (x + 0.34, y + 0.34), color=C["blue"], lw=1.2, ms=10)


def icon_wave(ax, x, y):
    ax.plot([x - 0.24, x - 0.16, x - 0.10, x - 0.05, x, x + 0.06, x + 0.13, x + 0.20],
            [y, y, y + 0.25, y - 0.20, y, y + 0.14, y, y], color=C["line"], linewidth=1.1)


def icon_refresh(ax, x, y):
    ax.add_patch(Arc((x, y), 0.46, 0.46, theta1=35, theta2=200, edgecolor=C["line"], linewidth=1.2))
    ax.add_patch(Arc((x, y), 0.46, 0.46, theta1=215, theta2=20, edgecolor=C["line"], linewidth=1.2))
    arrow(ax, (x - 0.20, y + 0.08), (x - 0.23, y + 0.20), color=C["line"], lw=1.0, ms=8)
    arrow(ax, (x + 0.20, y - 0.08), (x + 0.23, y - 0.20), color=C["line"], lw=1.0, ms=8)


def icon_cloud(ax, x, y):
    ax.add_patch(Circle((x - 0.12, y), 0.13, facecolor="white", edgecolor=C["line"], linewidth=1.0))
    ax.add_patch(Circle((x + 0.02, y + 0.07), 0.16, facecolor="white", edgecolor=C["line"], linewidth=1.0))
    ax.add_patch(Circle((x + 0.18, y), 0.12, facecolor="white", edgecolor=C["line"], linewidth=1.0))
    ax.plot([x - 0.24, x + 0.28], [y - 0.08, y - 0.08], color=C["line"], linewidth=1.0)
    txt(ax, x, y - 0.20, "⚡", size=17, color="#d97706")


def lambda_box(ax, x, y, name, hot=True):
    fc = C["warm"] if hot else C["cold"]
    ec = C["warm_edge"] if hot else C["cold_edge"]
    box(ax, x, y, 0.55, 0.42, fc=fc, ec=ec, lw=1.0, r=0.045)
    txt(ax, x + 0.275, y + 0.26, name, size=10, style="italic")
    txt(ax, x + 0.275, y + 0.10, "Hot" if hot else "Cold", size=7.4, color=C["ink"])


def step_box(ax, x, y, w, h, num, title, sub, ec, icon=None):
    box(ax, x, y, w, h, fc=C["white"], ec=ec, lw=1.0, r=0.075)
    if icon:
        icon(ax, x + 0.45, y + h / 2)
    txt(ax, x + 1.05, y + h * 0.62, f"{num}) {title}", size=10.2, weight="bold", ha="left")
    txt(ax, x + 1.05, y + h * 0.32, sub, size=8.0, color=C["muted"], ha="left")


def draw():
    plt.rcParams.update({"font.family": "DejaVu Sans", "svg.fonttype": "none", "pdf.fonttype": 42})
    fig, ax = plt.subplots(figsize=(16, 11), dpi=300)
    fig.patch.set_facecolor("white")
    ax.set_facecolor("white")
    ax.set_xlim(0, 16)
    ax.set_ylim(0, 11)
    ax.axis("off")

    # Top inputs band.
    box(ax, 0.35, 9.55, 15.1, 1.25, fc=C["panel"], ec=C["top_edge"], lw=1.2, r=0.12)
    txt(ax, 1.25, 10.38, "Workflow", size=12, weight="bold", color=C["blue"])
    txt(ax, 1.25, 10.02, "Layer / Inputs", size=12, weight="bold", color=C["blue"])

    box(ax, 2.45, 9.72, 2.90, 0.90, fc=C["white"], ec=C["top_edge"], lw=1.0, r=0.05)
    icon_dag(ax, 2.70, 10.17, s=0.28)
    txt(ax, 3.65, 10.38, "Workflow DAGs", size=10, weight="bold", ha="left")
    txt(ax, 3.65, 10.08, "chain / fanout /\nbranch / greedy-trap", size=8.2, ha="left", color=C["ink"])

    box(ax, 5.65, 9.72, 2.62, 0.90, fc=C["white"], ec=C["top_edge"], lw=1.0, r=0.05)
    icon_profile(ax, 5.88, 10.16)
    txt(ax, 6.68, 10.38, "Function profiles", size=10, weight="bold", ha="left")
    txt(ax, 6.68, 10.07, "execution time,\ncold-start,\nbranch probability", size=7.6, ha="left", color=C["ink"])

    box(ax, 8.58, 9.72, 2.85, 0.90, fc=C["white"], ec=C["top_edge"], lw=1.0, r=0.05)
    icon_gear(ax, 9.05, 10.17)
    txt(ax, 9.58, 10.34, "Platform parameters", size=10, weight="bold", ha="left")
    txt(ax, 10.20, 10.02, "$T_{ret}$,\nconcurrency limit", size=8.2, ha="center", color=C["ink"])

    box(ax, 11.68, 9.72, 1.88, 0.90, fc=C["white"], ec=C["top_edge"], lw=1.0, r=0.05)
    icon_budget(ax, 12.13, 10.17)
    txt(ax, 12.65, 10.34, "Budget B", size=10, weight="bold", ha="left")
    txt(ax, 12.65, 10.03, "warmup\nbudget", size=7.8, ha="left", color=C["ink"])

    icon_user(ax, 14.25, 10.18)
    txt(ax, 14.64, 10.35, "User /\nWorkflow\nRequest", size=8.4, weight="bold", ha="left")

    # MINT System.
    box(ax, 0.35, 4.78, 15.1, 4.55, fc="#fbfdff", ec="#0f49c6", lw=1.35, r=0.14)
    txt(ax, 8.0, 9.06, "MINT System", size=15, weight="bold", color=C["blue"])

    box(ax, 1.0, 5.02, 4.75, 3.80, fc="#f6faff", ec="#0f49c6", lw=1.1, r=0.12)
    txt(ax, 3.38, 8.50, "A. Offline Warmup Intent Planner", size=11.5, weight="bold", color=C["blue"])
    step_box(ax, 1.23, 7.55, 4.25, 0.78, 1, "Workflow & Profile Modeling", "DAG model, function profile", "#7aa9d6", icon_dag)
    step_box(ax, 1.23, 6.45, 4.25, 0.92, 2, "Hot/Cold State Estimation", "retention model,\nstate statistics", "#7aa9d6", icon_clock)
    step_box(ax, 1.23, 5.25, 4.25, 0.92, 3, "Benefit-aware Intent Planning", "target, time, expected benefit\nunder budget B", "#7aa9d6", icon_target)
    arrow(ax, (3.35, 7.55), (3.35, 7.37), color=C["line"], lw=1.0, ms=10)
    arrow(ax, (3.35, 6.45), (3.35, 6.17), color=C["line"], lw=1.0, ms=10)
    arrow(ax, (3.20, 6.45), (3.20, 6.17), color=C["line"], lw=1.0, ms=10, dashed=True)

    cylinder(ax, 6.45, 5.95, 1.70, 1.72, "Warmup\nIntent Store", "planned target /\ntime / benefit")
    arrow(ax, (5.75, 6.73), (6.45, 6.73), color=C["line"], lw=1.1, ms=12)

    box(ax, 8.82, 5.14, 5.25, 3.62, fc=C["green_fill"], ec=C["green"], lw=1.1, r=0.12)
    txt(ax, 11.45, 8.48, "B. Runtime-adaptive Orchestrator", size=11.5, weight="bold", color="#14551e")
    step_box(ax, 9.12, 7.88, 4.40, 0.63, 1, "State Tracker", "workflow progress, hot/cold state", "#7dbf83", icon_wave)
    step_box(ax, 9.12, 7.12, 4.40, 0.57, 2, "Intent Re-evaluator", "update benefit, check validity", "#7dbf83", icon_refresh)
    step_box(ax, 9.12, 5.90, 4.40, 1.02, 3, "Action Scheduler", "execute, cancel, replace, delay", "#7dbf83", None)
    for x, label in [(9.38, "Execute"), (10.18, "Cancel"), (11.24, "Replace"), (12.38, "Delay")]:
        box(ax, x, 6.03, 0.82, 0.28, fc="#f4fbf0", ec=C["green"], lw=0.8, r=0.06)
        txt(ax, x + 0.41, 6.17, label, size=7.4, color="#14551e", weight="bold")
    step_box(ax, 9.12, 5.10, 4.40, 0.57, 4, "Warmup Executor", "async warmup under budget B", "#7dbf83", icon_cloud)
    arrow(ax, (8.15, 6.73), (8.82, 6.73), color=C["line"], lw=1.1, ms=12)
    arrow(ax, (11.32, 7.88), (11.32, 7.69), color=C["line"], lw=1.0, ms=10)
    arrow(ax, (11.32, 7.12), (11.32, 6.92), color=C["line"], lw=1.0, ms=10)
    arrow(ax, (11.32, 5.90), (11.32, 5.67), color=C["line"], lw=1.0, ms=10)

    arrow(ax, (3.90, 9.72), (3.90, 9.33), color=C["line"], lw=1.0, ms=11)
    arrow(ax, (14.25, 9.74), (14.85, 4.78), color=C["line"], lw=1.0, ms=11, rad=0.0, label="Workflow\ninvocation", t=0.78, dx=-0.15, dy=0.05)
    arrow(ax, (11.32, 5.10), (11.32, 4.42), color=C["line"], lw=1.1, ms=12, label="Async warmup invocations", t=0.55, dx=-0.85)

    # Serverless platform.
    box(ax, 0.38, 2.20, 15.05, 1.90, fc=C["gray_fill"], ec=C["gray"], lw=1.2, r=0.14)
    txt(ax, 6.45, 3.85, "Serverless Platform", size=14, weight="bold")
    txt(ax, 8.65, 3.85, "(e.g., AWS Lambda)", size=9.5, weight="bold")

    box(ax, 0.55, 2.45, 2.38, 1.20, fc=C["white"], ec=C["gray"], lw=0.9, r=0.06)
    icon_dag(ax, 0.90, 3.02, s=0.20)
    txt(ax, 1.65, 3.38, "1) Workflow Invocation", size=8.5, weight="bold", ha="left")
    txt(ax, 1.65, 3.02, "real execution of\nworkflow DAG", size=7.4, color=C["muted"], ha="left")

    box(ax, 3.20, 2.45, 3.60, 1.20, fc=C["white"], ec=C["gray"], lw=0.9, r=0.06)
    txt(ax, 3.45, 3.38, r"$\lambda$", size=15)
    txt(ax, 3.82, 3.38, "2) Lambda Functions", size=8.5, weight="bold", ha="left")
    box(ax, 3.34, 2.65, 3.26, 0.68, fc="none", ec=C["gray"], lw=0.8, r=0.04)
    for x, name, hot in [(3.55, "$f_1$", True), (4.35, "$f_2$", False), (5.15, "$f_3$", True), (6.05, "$f_n$", False)]:
        lambda_box(ax, x, 2.78, name, hot=hot)
    txt(ax, 5.78, 3.00, "...", size=13)

    box(ax, 7.05, 2.45, 3.25, 1.20, fc=C["white"], ec=C["gray"], lw=0.9, r=0.06)
    txt(ax, 7.35, 3.38, "3) Instance Retention Window ($T_{ret}$)", size=8.5, weight="bold", ha="left")
    txt(ax, 7.26, 3.00, "❄", size=19, color="#2f78c4")
    rect(ax, 7.65, 2.83, 1.15, 0.25, fc=C["warm"], ec=C["warm_edge"], lw=0.8)
    txt(ax, 8.22, 2.96, "Warm", size=7.2, color="#14551e")
    arrow(ax, (8.80, 2.95), (9.52, 2.95), color=C["line"], lw=0.9, ms=8)
    rect(ax, 9.52, 2.83, 0.58, 0.25, fc="#f3f4f6", ec=C["gray"], lw=0.8)
    txt(ax, 9.81, 2.96, "Expire", size=6.2)
    ax.plot([7.65, 9.45], [2.64, 2.64], color=C["line"], linewidth=0.8)
    arrow(ax, (7.65, 2.64), (7.34, 2.64), color=C["line"], lw=0.8, ms=7)
    arrow(ax, (9.45, 2.64), (9.76, 2.64), color=C["line"], lw=0.8, ms=7)
    txt(ax, 8.55, 2.50, "$T_{ret}$", size=8.5)

    box(ax, 10.48, 2.45, 2.30, 1.20, fc=C["white"], ec=C["gray"], lw=0.9, r=0.06)
    txt(ax, 10.75, 3.38, "4) Concurrency Control", size=8.5, weight="bold", ha="left")
    txt(ax, 11.63, 3.08, "concurrency limit\nenforcement", size=7.0, color=C["muted"])
    for i in range(7):
        icon_user(ax, 10.70 + i * 0.22, 2.65)
    txt(ax, 12.02, 2.78, "...", size=12)

    box(ax, 13.00, 2.45, 2.18, 1.20, fc=C["white"], ec=C["gray"], lw=0.9, r=0.06)
    txt(ax, 13.24, 3.38, "5) Cold-start Events", size=8.5, weight="bold", ha="left")
    txt(ax, 13.35, 3.05, "❄", size=30, color="#2f78c4")
    txt(ax, 14.02, 3.03, "cold-starts,\nlatency spikes,\nwarmup outcomes", size=7.0, color=C["muted"], ha="left")

    # Feedback layer.
    box(ax, 0.38, 0.55, 15.05, 1.32, fc=C["purple_fill"], ec="#9c63c7", lw=1.1, r=0.13)
    txt(ax, 8.0, 1.68, "Observability & Feedback", size=13, weight="bold", color=C["purple"])
    fb = [
        (0.60, "1) Metrics Collection", "invocations, latency,\nconcurrency, cost", None),
        (4.20, "2) Cold-start Detection", "detect cold-start events\nand durations", None),
        (7.85, "3) Profile Calibration", "update function profiles,\nretention estimates", None),
        (11.65, "4) Performance Evaluation", "cold-start rate, latency,\ncost, benefit achieved", None),
    ]
    for x, title, sub, _ in fb:
        box(ax, x, 0.70, 2.96, 0.82, fc=C["white"], ec="#9c63c7", lw=0.9, r=0.06)
        txt(ax, x + 0.95, 1.28, title, size=8.3, weight="bold", ha="left")
        txt(ax, x + 0.95, 0.98, sub, size=6.8, color=C["muted"], ha="left")
    txt(ax, 0.98, 1.07, "▁▃▅█", size=24, color="#7b4fa3")
    ax.add_patch(Circle((4.77, 1.11), 0.27, facecolor="#f3e8ff", edgecolor="#7b4fa3", linewidth=1.0))
    txt(ax, 4.77, 1.11, "❄", size=18, color="#2f78c4")
    for yy in [1.30, 1.10, 0.90]:
        ax.plot([8.10, 8.95], [yy, yy], color="#7b4fa3", linewidth=1.2)
        ax.add_patch(Circle((8.32 if yy != 1.10 else 8.70, yy), 0.045, facecolor="#7b4fa3", edgecolor="#111827"))
    ax.plot([12.05, 12.35, 12.62, 12.82, 13.08], [0.92, 1.18, 1.08, 1.32, 1.45], color="#7b4fa3", linewidth=1.1, marker="o", markersize=2)
    arrow(ax, (3.56, 1.11), (4.20, 1.11), color=C["purple"], lw=1.1, ms=11)
    arrow(ax, (7.16, 1.11), (7.85, 1.11), color=C["purple"], lw=1.1, ms=11)
    arrow(ax, (10.80, 1.11), (11.65, 1.11), color=C["purple"], lw=1.1, ms=11)

    # Feedback arrows.
    arrow(ax, (1.90, 1.87), (1.90, 2.20), color=C["purple"], dashed=True, lw=1.0, ms=10)
    arrow(ax, (4.20, 1.87), (2.25, 5.25), color=C["purple"], dashed=True, lw=1.0, ms=10, rad=-0.15)
    arrow(ax, (11.95, 1.87), (11.95, 5.14), color=C["purple"], dashed=True, lw=1.0, ms=10)
    arrow(ax, (12.22, 1.87), (12.22, 4.78), color=C["purple"], dashed=True, lw=1.0, ms=10, rad=0.0)
    arrow(ax, (14.10, 2.45), (14.10, 1.87), color=C["line"], lw=1.0, ms=10)
    arrow(ax, (13.55, 7.88), (14.40, 7.88), color=C["line"], dashed=True, lw=1.0, ms=10)
    arrow(ax, (13.55, 7.12), (14.40, 7.12), color=C["line"], dashed=True, lw=1.0, ms=10)
    ax.plot([14.40, 14.40], [7.12, 7.88], color=C["line"], linewidth=1.0, linestyle=(0, (4, 4)))

    # Legend.
    box(ax, 10.25, 0.05, 4.60, 0.42, fc=C["white"], ec=C["line"], lw=0.9, r=0.03)
    arrow(ax, (10.42, 0.26), (11.00, 0.26), color=C["line"], lw=1.0, ms=9)
    txt(ax, 11.55, 0.26, "Control / Data Flow", size=7.4)
    arrow(ax, (12.88, 0.26), (13.48, 0.26), color=C["line"], dashed=True, lw=1.0, ms=9)
    txt(ax, 14.05, 0.26, "Feedback Flow", size=7.4)

    fig.savefig(SVG_PATH, bbox_inches="tight", facecolor="white")
    fig.savefig(PNG_PATH, bbox_inches="tight", facecolor="white", dpi=300)
    plt.close(fig)


if __name__ == "__main__":
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    draw()
    print(f"Wrote {SVG_PATH}")
    print(f"Wrote {PNG_PATH}")
