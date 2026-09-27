import { type HTMLAttributes, type ReactNode, useId } from 'react';
import { cx } from '../ui/primitives';

/**
 * One region of the principal detail pane: a sentence-case heading, a muted
 * note and an optional action on the ground, then its content. No box: the
 * detail pane separates regions with space. The heading sits under the
 * principal's `h2` name, so it defaults to `h3`.
 */
export function PrincipalSection({
  title,
  subtitle,
  action,
  titleId,
  headingLevel = 3,
  className,
  children,
  ...rest
}: {
  readonly title: ReactNode;
  readonly subtitle?: ReactNode;
  readonly action?: ReactNode;
  readonly titleId?: string;
  readonly headingLevel?: 2 | 3;
  readonly className?: string;
  readonly children?: ReactNode;
} & Omit<HTMLAttributes<HTMLElement>, 'title'>) {
  const generatedId = useId();
  const headingId = titleId ?? generatedId;
  const Heading = `h${headingLevel}` as const;
  return (
    <section
      aria-labelledby={headingId}
      data-principal-section=""
      className={cx('flex flex-col gap-4', className)}
      {...rest}
    >
      <div className="flex flex-col gap-3 sm:flex-row sm:items-start sm:justify-between sm:gap-4">
        <div className="min-w-0">
          <Heading id={headingId} className="text-title-section text-text">
            {title}
          </Heading>
          {subtitle ? (
            <div
              className="mt-1 min-h-5 text-body text-text-muted"
              data-slot="section-subtitle"
            >
              {subtitle}
            </div>
          ) : null}
        </div>
        {action ? (
          <div className="flex flex-wrap items-center gap-2 min-w-0 w-full sm:w-auto">
            {action}
          </div>
        ) : null}
      </div>
      {children}
    </section>
  );
}
