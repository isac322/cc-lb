interface SectionNavProps {
  sections: string[];
  activeSection: string;
  onSelect: (section: string) => void;
}

export function SectionNav({ sections, activeSection, onSelect }: SectionNavProps) {
  return (
    <nav className="space-y-1">
      {sections.map((section) => (
        <button
          key={section}
          onClick={() => onSelect(section)}
          className={`w-full text-left px-3 py-2 rounded-md text-sm font-medium transition-colors ${
            activeSection === section
              ? 'bg-graphite-800 text-graphite-50'
              : 'text-graphite-400 hover:bg-graphite-800/50 hover:text-graphite-200'
          }`}
        >
          {section.charAt(0).toUpperCase() + section.slice(1).replace('_', ' ')}
        </button>
      ))}
    </nav>
  );
}
