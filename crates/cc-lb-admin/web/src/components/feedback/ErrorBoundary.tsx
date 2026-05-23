import { Component, ErrorInfo, ReactNode } from 'react';
import { ErrorState } from '../primitives/ErrorState';

interface Props {
  children: ReactNode;
}

interface State {
  hasError: boolean;
  error: Error | null;
}

export class ErrorBoundary extends Component<Props, State> {
  public state: State = {
    hasError: false,
    error: null,
  };

  public static getDerivedStateFromError(error: Error): State {
    return { hasError: true, error };
  }

  public componentDidCatch(error: Error, errorInfo: ErrorInfo) {
    console.error('Uncaught error:', error, errorInfo);
  }

  public render() {
    if (this.state.hasError) {
      return (
        <div className="min-h-screen bg-graphite-900 flex items-center justify-center p-4">
          <div className="max-w-md w-full bg-graphite-850 border border-graphite-800 rounded-lg shadow-xl p-6">
            <ErrorState
              title="Something went wrong"
              message={this.state.error?.message || 'An unexpected error occurred.'}
              onRetry={() => {
                this.setState({ hasError: false, error: null });
                window.location.reload();
              }}
            />
          </div>
        </div>
      );
    }

    return this.props.children;
  }
}
