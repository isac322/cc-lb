import { NAV_GROUPS } from '../../components/layout/navItems';
import type { Design } from '../types';
import OverviewPage from './OverviewPage';
import { PrincipalsActivity } from './PrincipalsActivity';

const defaultDesign: Design = {
  id: 'default',
  label: 'Current admin',
  navItems: () =>
    NAV_GROUPS.map((group) => ({
      ...group,
      items: group.items.map((item) => ({
        ...item,
        mobile: ['/', '/upstreams', '/principals', '/logs'].includes(item.path),
        mobileOrder: ['/', '/upstreams', '/principals', '/logs'].indexOf(
          item.path,
        ),
      })),
    })),
  OverviewPage,
  PrincipalsActivity,
  UpstreamsListAddon: { groupBy: 'none' },
};
export default defaultDesign;
