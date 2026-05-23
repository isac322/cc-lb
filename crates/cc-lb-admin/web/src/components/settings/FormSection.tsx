import type { ReactNode } from 'react';
import { Card } from '../primitives/Card';

interface FormSectionProps {
  id: string;
  title: string;
  description?: string;
  children: ReactNode;
}

export function FormSection({
  id,
  title,
  description,
  children,
}: FormSectionProps) {
  return (
    <section id={id} className="mb-8 scroll-mt-6">
      <Card className="p-6">
        <div className="mb-6">
          <h3 className="text-lg font-semibold text-graphite-50">{title}</h3>
          {description && (
            <p className="text-sm text-graphite-400 mt-1">{description}</p>
          )}
        </div>
        <div className="space-y-6">{children}</div>
      </Card>
    </section>
  );
}
