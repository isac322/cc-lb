import { createFileRoute } from '@tanstack/react-router';
import { AlertTriangle, Power } from 'lucide-react';
import { useState } from 'react';
import { toast } from 'sonner';
import {
  Badge,
  Button,
  Card,
  CardBody,
  CardHeader,
  ConfirmDialog,
  cx,
  PageContainer,
  Section,
  Skeleton,
} from '../components/ui/primitives';
import { RelativeTime } from '../components/ui/RelativeTime';
import { useKillswitch, useStatus } from '../lib/queries';

export const Route = createFileRoute('/status')({
  component: StatusPage,
});

const SYSTEM_CARD_CLASS = 'flex min-h-[144px] flex-col';
const LAST_RELOAD_VALUE_SLOT_CLASS = 'min-w-20';
const PLUGIN_COUNT_VALUE_SLOT_CLASS = 'min-w-8';
const KILLSWITCH_STATUS_SLOT_CLASS =
  'inline-flex min-h-[22px] min-w-14 items-center justify-end text-right';

function StatusPage() {
  const status = useStatus();
  const kill = useKillswitch();
  const [confirmEngageOpen, setConfirmEngageOpen] = useState(false);
  // One killswitch request may be in flight at a time and it locks both entry
  // points. The mutation's own variables name the requested direction, so the
  // pending label stays stable while `status` refetches behind it.
  const killRequestedEnable = kill.variables ?? !status.data?.killswitch;
  const killEngagePending = kill.isPending && killRequestedEnable;
  const killDisengagePending = kill.isPending && !killRequestedEnable;

  return (
    <PageContainer>
      <Section
        title="Restart-Required Changes"
        subtitle="Fields needing process restart"
      >
        <Card data-testid="restart-required-card">
          <CardBody
            data-testid="restart-required-body"
            className="min-h-[72px]"
          >
            {status.isLoading ? (
              <div className="space-y-2">
                <Skeleton className="h-4 w-1/2" />
                <Skeleton className="h-4 w-2/3" />
              </div>
            ) : status.data ? (
              status.data.restart_required_changes.length ? (
                <ul className="space-y-2">
                  {status.data.restart_required_changes.map((c, i) => (
                    <li key={i} className="text-xs flex items-start gap-2">
                      <AlertTriangle className="w-4 h-4 text-amber-400 shrink-0" />
                      <div>
                        <div className="font-mono">{c.field}</div>
                        <div className="text-text-faint">
                          {c.current} → {c.new} · {c.reason}
                        </div>
                      </div>
                    </li>
                  ))}
                </ul>
              ) : (
                <p className="text-xs text-text-faint">
                  All hot-applied. No restart required.
                </p>
              )
            ) : null}
          </CardBody>
        </Card>
      </Section>

      <Section title="System">
        <div
          data-testid="system-grid"
          className="grid grid-cols-1 md:grid-cols-3 gap-4"
        >
          <Card className={SYSTEM_CARD_CLASS} data-testid="system-card">
            <CardHeader title="Storage Backend" />
            <CardBody className="flex-1 space-y-2 text-xs">
              <Row label="Kind" value="postgres" />
              <Row label="Pool" value="10/10 idle" />
              <Row
                label="Last reload"
                valueClassName={LAST_RELOAD_VALUE_SLOT_CLASS}
                valueTestId="system-last-reload-slot"
                value={
                  status.isLoading ? (
                    <Skeleton className="h-4 w-full" />
                  ) : (
                    <RelativeTime
                      ts={
                        status.data?.last_reload_status?.applied_at_unix_secs
                          ? status.data.last_reload_status
                              .applied_at_unix_secs * 1000
                          : null
                      }
                    />
                  )
                }
              />
            </CardBody>
          </Card>
          <Card className={SYSTEM_CARD_CLASS} data-testid="system-card">
            <CardHeader title="Plugin Chain Summary" />
            <CardBody className="flex-1 space-y-2 text-xs">
              <Row
                label="Principals w/ chain"
                valueClassName={PLUGIN_COUNT_VALUE_SLOT_CLASS}
                valueTestId="system-principal-chain-count-slot"
                value={
                  status.isLoading ? (
                    <Skeleton className="h-4 w-full" />
                  ) : (
                    (status.data?.plugin_chain_summary
                      .principal_count_with_chain ?? 0)
                  )
                }
              />
              <Row
                label="Total entries"
                valueClassName={PLUGIN_COUNT_VALUE_SLOT_CLASS}
                valueTestId="system-total-chain-count-slot"
                value={
                  status.isLoading ? (
                    <Skeleton className="h-4 w-full" />
                  ) : (
                    (status.data?.plugin_chain_summary.total_entries ?? 0)
                  )
                }
              />
            </CardBody>
          </Card>
          <Card className={SYSTEM_CARD_CLASS} data-testid="system-card">
            <CardHeader
              title="Killswitch"
              subtitle="Emergency stop for all traffic"
            />
            <CardBody className="flex flex-1 flex-col space-y-3">
              <div className="flex min-h-[22px] items-center justify-between text-xs">
                <span className="text-text-faint">Status</span>
                <div
                  data-testid="system-killswitch-status-slot"
                  className={KILLSWITCH_STATUS_SLOT_CLASS}
                >
                  {status.isLoading ? (
                    <Skeleton className="h-5 w-full" />
                  ) : (
                    <Badge tone={status.data?.killswitch ? 'danger' : 'ok'}>
                      {status.data?.killswitch ? 'ACTIVE' : 'OFF'}
                    </Badge>
                  )}
                </div>
              </div>
              <Button
                fullWidth
                size="sm"
                className="mt-auto"
                data-testid="killswitch-control"
                aria-label={
                  status.isLoading ? 'Killswitch status loading' : undefined
                }
                disabled={status.isLoading || kill.isPending}
                loading={kill.isPending}
                variant={
                  status.isLoading
                    ? 'secondary'
                    : status.data?.killswitch
                      ? 'secondary'
                      : 'danger'
                }
                iconLeft={<Power className="w-3 h-3" />}
                onClick={() => {
                  if (status.data?.killswitch) {
                    kill.mutate(false, {
                      onSuccess: () => toast.success('Killswitch disengaged'),
                    });
                  } else {
                    setConfirmEngageOpen(true);
                  }
                }}
              >
                {status.isLoading ? (
                  <Skeleton className="h-3 w-24" />
                ) : killDisengagePending ? (
                  'Disengaging...'
                ) : killEngagePending ? (
                  'Engaging...'
                ) : status.data?.killswitch ? (
                  'Disengage'
                ) : (
                  'Engage killswitch'
                )}
              </Button>
            </CardBody>
          </Card>
        </div>
      </Section>

      <ConfirmDialog
        open={confirmEngageOpen}
        onOpenChange={setConfirmEngageOpen}
        title="Engage killswitch?"
        description="All proxy traffic will stop immediately. Inbound requests will be rejected until the killswitch is disengaged."
        confirmLabel={killEngagePending ? 'Engaging...' : 'Engage killswitch'}
        destructive
        pending={kill.isPending}
        closeOnConfirm={false}
        onConfirm={() =>
          kill.mutate(true, {
            onSuccess: () => {
              setConfirmEngageOpen(false);
              toast.success('Killswitch engaged');
            },
          })
        }
      />
    </PageContainer>
  );
}

function Row({
  label,
  value,
  valueClassName,
  valueTestId,
}: {
  label: string;
  value: React.ReactNode;
  valueClassName?: string;
  valueTestId?: string;
}) {
  return (
    <div className="flex min-h-[22px] items-center justify-between">
      <span className="text-text-faint">{label}</span>
      <div
        className={cx(
          'inline-flex min-h-[22px] items-center justify-end text-right',
          valueClassName,
        )}
        data-testid={valueTestId}
      >
        {value}
      </div>
    </div>
  );
}
