import { NavLink } from 'react-router';
import {
  IconChart,
  IconFile,
  IconGear,
  IconLines,
  IconList,
  IconLock,
  IconPlug,
  IconServer,
  IconSquare,
} from './icons';

const NAV_ITEMS = [
  { path: '/', label: 'Overview', icon: IconSquare },
  { path: '/principals', label: 'Limits', icon: IconList },
  { path: '/log', label: 'Live Log', icon: IconLines },
  { path: '/management', label: 'Management', icon: IconFile },
  { path: '/settings', label: 'Settings', icon: IconGear },
  { path: '/upstreams', label: 'Upstreams', icon: IconServer },
  { path: '/activity', label: 'Audit', icon: IconChart },
  { path: '/credentials', label: 'Credentials', icon: IconLock },
  { path: '/plugins', label: 'Plugins', icon: IconPlug },
];

export function Sidebar() {
  return (
    <aside className="w-14 lg:w-56 flex-shrink-0 border-r border-graphite-800 bg-graphite-900 h-screen sticky top-0 overflow-y-auto flex flex-col transition-all duration-150">
      <div className="h-16 flex flex-col items-center justify-center lg:items-start lg:justify-center lg:px-6 border-b border-graphite-800">
        <div className="flex items-center">
          <div className="w-8 h-8 bg-cyan-500 rounded flex items-center justify-center text-graphite-950 font-bold">
            cc
          </div>
          <span className="ml-3 font-semibold text-graphite-50 hidden lg:block tracking-tight">
            cc-lb
          </span>
        </div>
        <span className="hidden lg:block font-mono text-[10px] text-graphite-400 uppercase tracking-[0.2em] mt-1">
          console
        </span>
      </div>
      <nav className="flex-1 py-4 flex flex-col gap-1 px-2">
        {NAV_ITEMS.map((item) => (
          <NavLink
            key={item.path}
            to={item.path}
            className={({ isActive }) =>
              `flex items-center gap-3 px-3 py-2 text-sm rounded-md transition-colors focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-cyan-500 ${
                isActive
                  ? 'bg-graphite-800/80 text-cyan-500 border-l-2 border-cyan-500'
                  : 'text-graphite-400 hover:text-graphite-100 hover:bg-graphite-850/60 border-l-2 border-transparent'
              }`
            }
            title={item.label}
          >
            <item.icon className="w-5 h-5 flex-shrink-0" />
            <span className="hidden lg:block font-medium whitespace-nowrap">
              {item.label}
            </span>
          </NavLink>
        ))}
      </nav>
    </aside>
  );
}
