import { useSyncExternalStore } from 'react';

export interface VisibilityState {
  visible: boolean;
  online: boolean;
  hiddenSince: number | null;
  gracePeriodElapsed: boolean;
}

type Listener = () => void;

class VisibilityManager {
  private state: VisibilityState;
  private listeners: Set<Listener> = new Set();
  private graceTimer: ReturnType<typeof setTimeout> | null = null;

  constructor() {
    const isBrowser =
      typeof window !== 'undefined' && typeof document !== 'undefined';
    this.state = {
      visible: isBrowser ? document.visibilityState === 'visible' : true,
      online: isBrowser ? navigator.onLine : true,
      hiddenSince: null,
      gracePeriodElapsed: false,
    };

    if (isBrowser) {
      document.addEventListener(
        'visibilitychange',
        this.handleVisibilityChange,
      );
      window.addEventListener('online', this.handleOnline);
      window.addEventListener('offline', this.handleOffline);

      if (!this.state.visible) {
        this.startGraceTimer();
      }
    }
  }

  private notify = () => {
    for (const listener of this.listeners) {
      listener();
    }
  };

  private startGraceTimer = () => {
    if (this.graceTimer) clearTimeout(this.graceTimer);
    this.state = {
      ...this.state,
      hiddenSince: Date.now(),
      gracePeriodElapsed: false,
    };
    this.graceTimer = setTimeout(() => {
      this.state = { ...this.state, gracePeriodElapsed: true };
      this.notify();
    }, 120_000);
  };

  private clearGraceTimer = () => {
    if (this.graceTimer) {
      clearTimeout(this.graceTimer);
      this.graceTimer = null;
    }
    this.state = {
      ...this.state,
      hiddenSince: null,
      gracePeriodElapsed: false,
    };
  };

  private handleVisibilityChange = () => {
    const visible = document.visibilityState === 'visible';
    if (visible === this.state.visible) return;

    if (visible) {
      this.clearGraceTimer();
      this.state = { ...this.state, visible: true };
    } else {
      this.state = { ...this.state, visible: false };
      this.startGraceTimer();
    }
    this.notify();
  };

  private handleOnline = () => {
    if (this.state.online) return;
    this.state = { ...this.state, online: true };
    this.notify();
  };

  private handleOffline = () => {
    if (!this.state.online) return;
    this.state = { ...this.state, online: false };
    this.notify();
  };

  subscribe = (listener: Listener) => {
    this.listeners.add(listener);
    return () => {
      this.listeners.delete(listener);
    };
  };

  getState = () => this.state;
  snapshot = () => this.state;
}

export const visibilityManager = new VisibilityManager();

export function useVisibility(): VisibilityState {
  return useSyncExternalStore(
    visibilityManager.subscribe,
    visibilityManager.snapshot,
    visibilityManager.snapshot,
  );
}
