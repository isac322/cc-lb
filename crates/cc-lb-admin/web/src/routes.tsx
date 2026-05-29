import { lazy } from 'react';
import { Route, Routes } from 'react-router';
import { ErrorState } from './components/primitives/ErrorState';

const Overview = lazy(() => import('./pages/Overview'));
const PrincipalLimits = lazy(() => import('./pages/PrincipalLimits'));
const RealtimeLog = lazy(() => import('./pages/RealtimeLog'));
const PrincipalManagement = lazy(() => import('./pages/PrincipalManagement'));
const Settings = lazy(() => import('./pages/Settings'));
const Upstreams = lazy(() => import('./pages/Upstreams'));
const AdminActivity = lazy(() => import('./pages/AdminActivity'));
const CredentialStatus = lazy(() => import('./pages/CredentialStatus'));
const PluginRegistry = lazy(() => import('./pages/PluginRegistry'));

function NotFound() {
  return (
    <div className="flex items-center justify-center h-full">
      <ErrorState
        title="404 Not Found"
        message="The page you are looking for does not exist."
        onRetry={() => (window.location.href = '/')}
      />
    </div>
  );
}

export function AppRoutes() {
  return (
    <Routes>
      <Route path="/" element={<Overview />} />
      <Route path="/principals" element={<PrincipalLimits />} />
      <Route path="/log" element={<RealtimeLog />} />
      <Route path="/management" element={<PrincipalManagement />} />
      <Route path="/settings" element={<Settings />} />
      <Route path="/upstreams" element={<Upstreams />} />
      <Route path="/activity" element={<AdminActivity />} />
      <Route path="/credentials" element={<CredentialStatus />} />
      <Route path="/plugins" element={<PluginRegistry />} />
      <Route path="*" element={<NotFound />} />
    </Routes>
  );
}
