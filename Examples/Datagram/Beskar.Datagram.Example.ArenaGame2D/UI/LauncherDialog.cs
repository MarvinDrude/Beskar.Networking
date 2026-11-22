using System.Drawing.Drawing2D;
using System.Drawing.Imaging;
using Beskar.Datagram.Example.ArenaGame2D.Protocol;

namespace Beskar.Datagram.Example.ArenaGame2D.UI;

/// <summary>
/// Dark-mode launcher dialog (AutoScaleMode.None + custom vector buttons + dark Windows title bar).
/// </summary>
internal sealed class LauncherDialog : Form
{
   private static readonly RectangleF BtnPairRect = new(32, 202, 456, 42);
   private static readonly RectangleF BtnHostRect = new(32, 256, 222, 40);
   private static readonly RectangleF BtnJoinRect = new(266, 256, 222, 40);

   private readonly TextBox _txtName = new();
   private readonly TextBox _txtAddress = new();

   private readonly Font _fTitle = UiTheme.MakeFont("Segoe UI", 20f, FontStyle.Bold);
   private readonly Font _fSub = UiTheme.MakeFont("Segoe UI", 12.5f);
   private readonly Font _fLabel = UiTheme.MakeFont("Segoe UI", 12.5f, FontStyle.Bold);
   private readonly Font _fInput = UiTheme.MakeFont("Consolas", 13.5f);
   private readonly Font _fBtn = UiTheme.MakeFont("Segoe UI", 13f, FontStyle.Bold);

   private PointF _cursor;

   public bool IsHost { get; private set; }
   public bool LaunchPair { get; private set; }
   public int Port { get; private set; } = GameProtocol2D.DefaultPort;

   public string PlayerName => string.IsNullOrWhiteSpace(_txtName.Text) ? "Pilot" : _txtName.Text.Trim();
   public string JoinAddress => string.IsNullOrWhiteSpace(_txtAddress.Text) ? $"127.0.0.1:{GameProtocol2D.DefaultPort}" : _txtAddress.Text.Trim();

   public LauncherDialog()
   {
      AutoScaleMode = AutoScaleMode.None;
      DoubleBuffered = true;
      SetStyle(ControlStyles.AllPaintingInWmPaint | ControlStyles.UserPaint | ControlStyles.OptimizedDoubleBuffer, true);

      Text = "Beskar Arena 2D — Launcher";
      ClientSize = new Size(520, 322);
      FormBorderStyle = FormBorderStyle.FixedDialog;
      MaximizeBox = false;
      MinimizeBox = false;
      StartPosition = FormStartPosition.CenterScreen;
      BackColor = UiTheme.WindowBg;
      ForeColor = UiTheme.TextPrimary;

      ConfigureInputs();
      WireMouseEvents();
   }

   public void CaptureToFile(string path)
   {
      using var bmp = new Bitmap(ClientSize.Width, ClientSize.Height, PixelFormat.Format32bppArgb);
      using (var g = Graphics.FromImage(bmp))
      {
         PaintDialogSurface(g, drawTextBoxesManually: true);
      }

      Directory.CreateDirectory(Path.GetDirectoryName(path)!);
      bmp.Save(path, ImageFormat.Png);
   }

   protected override void OnHandleCreated(EventArgs e)
   {
      base.OnHandleCreated(e);
      UiTheme.EnableDarkTitleBar(Handle);
   }

   protected override void OnPaint(PaintEventArgs e)
   {
      PaintDialogSurface(e.Graphics, drawTextBoxesManually: false);
   }

   protected override void Dispose(bool disposing)
   {
      if (disposing)
      {
         _fTitle.Dispose();
         _fSub.Dispose();
         _fLabel.Dispose();
         _fInput.Dispose();
         _fBtn.Dispose();
      }

      base.Dispose(disposing);
   }

   private void ConfigureInputs()
   {
      _txtName.Text = "Alice";
      _txtName.Font = _fInput;
      _txtName.Location = new Point(166, 108);
      _txtName.Size = new Size(312, 26);
      _txtName.BackColor = UiTheme.CardBg;
      _txtName.ForeColor = UiTheme.TextPrimary;
      _txtName.BorderStyle = BorderStyle.None;

      _txtAddress.Text = $"127.0.0.1:{GameProtocol2D.DefaultPort}";
      _txtAddress.Font = _fInput;
      _txtAddress.Location = new Point(166, 152);
      _txtAddress.Size = new Size(312, 26);
      _txtAddress.BackColor = UiTheme.CardBg;
      _txtAddress.ForeColor = UiTheme.TextPrimary;
      _txtAddress.BorderStyle = BorderStyle.None;

      Controls.AddRange([_txtName, _txtAddress]);
   }

   private void WireMouseEvents()
   {
      MouseMove += (_, e) =>
      {
         _cursor = new PointF(e.X, e.Y);
         Cursor = BtnPairRect.Contains(_cursor) || BtnHostRect.Contains(_cursor) || BtnJoinRect.Contains(_cursor)
            ? Cursors.Hand
            : Cursors.Default;
         Invalidate();
      };

      MouseDown += (_, e) =>
      {
         if (e.Button != MouseButtons.Left) return;

         var pt = new PointF(e.X, e.Y);

         if (BtnPairRect.Contains(pt))
         {
            LaunchPair = true;
            IsHost = true;
            Port = ExtractPort();
            DialogResult = DialogResult.OK;
         }
         else if (BtnHostRect.Contains(pt))
         {
            IsHost = true;
            Port = ExtractPort();
            DialogResult = DialogResult.OK;
         }
         else if (BtnJoinRect.Contains(pt))
         {
            IsHost = false;
            DialogResult = DialogResult.OK;
         }
      };
   }

   private void PaintDialogSurface(Graphics g, bool drawTextBoxesManually)
   {
      g.SmoothingMode = SmoothingMode.AntiAlias;
      g.TextRenderingHint = System.Drawing.Text.TextRenderingHint.ClearTypeGridFit;
      g.Clear(UiTheme.WindowBg);

      // Top matte header strip
      using (var hdr = new SolidBrush(UiTheme.HeaderBg))
         g.FillRectangle(hdr, 0, 0, ClientSize.Width, 76);

      using (var div = new Pen(UiTheme.BorderSubtle, 1f))
         g.DrawLine(div, 0, 76, ClientSize.Width, 76);

      using var tb = new SolidBrush(UiTheme.TextPrimary);
      using var sb = new SolidBrush(UiTheme.TextSecondary);

      g.DrawString("Beskar Arena 2D", _fTitle, tb, 32f, 16f);
      g.DrawString("Beskar.Networking.Datagram  |  UDP + AEAD  |  Netcode Demo", _fSub, sb, 32f, 46f);

      // Clean bordered input boxes
      var nameBox = new RectangleF(156, 100, 332, 34);
      var addrBox = new RectangleF(156, 144, 332, 34);

      UiTheme.FillRoundedRect(g, nameBox, 4f, UiTheme.CardBg);
      UiTheme.DrawRoundedRect(g, nameBox, 4f, UiTheme.BorderStrong, 1f);

      UiTheme.FillRoundedRect(g, addrBox, 4f, UiTheme.CardBg);
      UiTheme.DrawRoundedRect(g, addrBox, 4f, UiTheme.BorderStrong, 1f);

      g.DrawString("Player name", _fLabel, sb, 32f, 109f);
      g.DrawString("Host address", _fLabel, sb, 32f, 153f);

      if (drawTextBoxesManually)
      {
         g.DrawString(_txtName.Text, _fInput, tb, 166f, 109f);
         g.DrawString(_txtAddress.Text, _fInput, tb, 166f, 153f);
      }

      UiTheme.DrawFlatButton(g, BtnPairRect, "Launch Host + Client Pair", _fBtn, active: false, hovered: BtnPairRect.Contains(_cursor), primary: true);
      UiTheme.DrawFlatButton(g, BtnHostRect, "Host Only", _fBtn, active: false, hovered: BtnHostRect.Contains(_cursor));
      UiTheme.DrawFlatButton(g, BtnJoinRect, "Join Host", _fBtn, active: false, hovered: BtnJoinRect.Contains(_cursor));
   }

   private int ExtractPort()
   {
      var parts = JoinAddress.Split(':', 2);
      return parts.Length == 2 && int.TryParse(parts[1], out var p) ? p : GameProtocol2D.DefaultPort;
   }
}
