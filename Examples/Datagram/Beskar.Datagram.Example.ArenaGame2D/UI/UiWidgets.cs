using Beskar.Datagram.Example.ArenaGame2D.Protocol;

namespace Beskar.Datagram.Example.ArenaGame2D.UI;

internal readonly record struct VisualRing(float X, float Y, FxKind2D Kind, Color Color, float Age, float MaxAge);

internal readonly record struct FloatingPopup(float X, float Y, string Text, Color Color, float Age, float MaxAge);

internal readonly record struct TrailPoint(float X, float Y);

internal sealed class ButtonWidget(
   string id,
   RectangleF rect,
   Func<string> text,
   Func<bool> isActive,
   Action onClick,
   bool primary = false)
{
   public string Id { get; } = id;
   public RectangleF Rect { get; } = rect;
   public Func<string> Text { get; } = text;
   public Func<bool> IsActive { get; } = isActive;
   public Action OnClick { get; } = onClick;
   public bool Primary { get; } = primary;
}

internal sealed class SliderWidget(
   string id,
   string title,
   RectangleF hitRect,
   RectangleF trackRect,
   int min,
   int max,
   Func<int> getter,
   Action<int> setter,
   Func<int, string> formatValue)
{
   public string Id { get; } = id;
   public string Title { get; } = title;
   public RectangleF HitRect { get; } = hitRect;
   public RectangleF TrackRect { get; } = trackRect;
   public int Min { get; } = min;
   public int Max { get; } = max;
   public Func<int> Getter { get; } = getter;
   public Action<int> Setter { get; } = setter;
   public Func<int, string> FormatValue { get; } = formatValue;
}

internal sealed class ToggleWidget(
   string id,
   string label,
   string hotkey,
   RectangleF rowRect,
   RectangleF checkRect,
   Func<bool> getter,
   Action<bool> setter)
{
   public string Id { get; } = id;
   public string Label { get; } = label;
   public string Hotkey { get; } = hotkey;
   public RectangleF RowRect { get; } = rowRect;
   public RectangleF CheckRect { get; } = checkRect;
   public Func<bool> Getter { get; } = getter;
   public Action<bool> Setter { get; } = setter;
}

/// <summary>
/// Owns all pixel-exact GDI+ fonts used by <see cref="GameCanvasForm"/> so creation and disposal live in one place.
/// </summary>
internal sealed class UiFonts : IDisposable
{
   public Font Brand { get; } = UiTheme.MakeFont("Segoe UI", 16f, FontStyle.Bold);
   public Font CardTitle { get; } = UiTheme.MakeFont("Segoe UI", 12f, FontStyle.Bold);
   public Font Body { get; } = UiTheme.MakeFont("Segoe UI", 12.5f, FontStyle.Regular);
   public Font BodyBold { get; } = UiTheme.MakeFont("Segoe UI", 12.5f, FontStyle.Bold);
   public Font Button { get; } = UiTheme.MakeFont("Segoe UI", 12f, FontStyle.Bold);
   public Font Badge { get; } = UiTheme.MakeFont("Consolas", 10.5f, FontStyle.Bold);
   public Font Mono { get; } = UiTheme.MakeFont("Consolas", 12f, FontStyle.Regular);
   public Font MonoBold { get; } = UiTheme.MakeFont("Consolas", 12f, FontStyle.Bold);
   public Font MonoSmall { get; } = UiTheme.MakeFont("Consolas", 11f, FontStyle.Regular);
   public Font MonoSmallBold { get; } = UiTheme.MakeFont("Consolas", 11f, FontStyle.Bold);
   public Font Popup { get; } = UiTheme.MakeFont("Segoe UI", 13f, FontStyle.Bold);

   public void Dispose()
   {
      Brand.Dispose();
      CardTitle.Dispose();
      Body.Dispose();
      BodyBold.Dispose();
      Button.Dispose();
      Badge.Dispose();
      Mono.Dispose();
      MonoBold.Dispose();
      MonoSmall.Dispose();
      MonoSmallBold.Dispose();
      Popup.Dispose();
   }
}
