export function Dialog({
  open,
  children,
}: {
  open?: boolean;
  children?: React.ReactNode;
  onOpenChange?: (open: boolean) => void;
}) {
  return open ? <div className="dialog">{children}</div> : null;
}

export function DialogContent({ children }: { children?: React.ReactNode }) {
  return <div className="dialog-content">{children}</div>;
}

export function DialogHeader({ children }: { children?: React.ReactNode }) {
  return <div className="dialog-header">{children}</div>;
}

export function DialogTitle({ children }: { children?: React.ReactNode }) {
  return <h2 className="dialog-title">{children}</h2>;
}

export function DialogDescription({
  children,
}: {
  children?: React.ReactNode;
}) {
  return <p className="dialog-description">{children}</p>;
}

export function DialogFooter({ children }: { children?: React.ReactNode }) {
  return <div className="dialog-footer">{children}</div>;
}
