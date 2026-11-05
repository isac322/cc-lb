import { useNavigate } from '@tanstack/react-router';
import type { ComponentProps, MouseEvent } from 'react';
import { TableRow } from '../components/ui/Table';

/** Keep the native name link as the tab stop and extend its target to the row. */
export function PrincipalUsageRow({
  principalId,
  ...props
}: ComponentProps<typeof TableRow> & { principalId: string }) {
  const navigate = useNavigate();
  const activate = (event: MouseEvent<HTMLTableRowElement>) => {
    if (
      event.defaultPrevented ||
      (event.type === 'auxclick' ? event.button !== 1 : event.button !== 0) ||
      !(event.target instanceof Element) ||
      event.target.closest(
        'a, button, input, select, textarea, [role="button"]',
      ) ||
      window.getSelection()?.isCollapsed === false
    ) {
      return;
    }

    if (
      event.button === 1 ||
      event.metaKey ||
      event.ctrlKey ||
      event.shiftKey
    ) {
      const link = event.currentTarget.querySelector<HTMLAnchorElement>(
        'a[data-principal-link]',
      );
      if (link) {
        event.preventDefault();
        window.open(link.href, '_blank', 'noopener,noreferrer');
      }
      return;
    }

    void navigate({ to: '/principals', search: { selectedId: principalId } });
  };

  return (
    <TableRow
      {...props}
      onClickCapture={activate}
      onAuxClickCapture={activate}
    />
  );
}
