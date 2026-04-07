import { ReactNode, useState } from "react";
import { Link, useLocation } from "wouter";
import { cn } from "@/lib/utils";
import { BarChart3, Calculator, LineChart, Menu, X, Terminal } from "lucide-react";
import { usePortfolio } from "@/hooks/use-portfolio";
import { Sheet, SheetContent, SheetTrigger } from "@/components/ui/sheet";

const NAV_LINKS = [
  { href: "/", label: "Universe Rankings", icon: BarChart3 },
  { href: "/portfolio", label: "Portfolio & Risk", icon: LineChart },
  { href: "/methodology", label: "Methodology", icon: Calculator },
];

function NavLinks({ onNavigate }: { onNavigate?: () => void }) {
  const [location] = useLocation();
  const { basket } = usePortfolio();

  return (
    <nav className="flex flex-col gap-1 p-3">
      {NAV_LINKS.map(({ href, label, icon: Icon }) => {
        const active = location === href;
        const showBadge = href === "/portfolio" && basket.length > 0;
        return (
          <Link
            key={href}
            href={href}
            onClick={onNavigate}
            className={cn(
              "flex items-center gap-3 px-3 py-2.5 text-sm rounded-md transition-colors",
              active
                ? "bg-sidebar-accent text-sidebar-accent-foreground font-medium"
                : "text-sidebar-foreground/70 hover:bg-sidebar-accent/50 hover:text-sidebar-accent-foreground"
            )}
          >
            <Icon className="w-4 h-4 shrink-0" />
            <span className="flex-1">{label}</span>
            {showBadge && (
              <span className="bg-primary/20 text-primary text-[10px] font-bold px-2 py-0.5 rounded-full">
                {basket.length}
              </span>
            )}
          </Link>
        );
      })}
    </nav>
  );
}

function DesktopSidebar() {
  return (
    <aside className="hidden lg:flex w-56 border-r border-border bg-sidebar flex-col h-screen shrink-0 sticky top-0">
      <div className="h-12 flex items-center gap-2 px-4 border-b border-border font-bold text-sm text-primary tracking-widest uppercase">
        <Terminal className="w-4 h-4" />
        Quant Terminal
      </div>
      <div className="flex-1 overflow-y-auto">
        <NavLinks />
      </div>
      <div className="p-4 text-xs text-muted-foreground border-t border-border">
        v1.0.0-beta
      </div>
    </aside>
  );
}

function MobileHeader() {
  const [open, setOpen] = useState(false);

  return (
    <header className="lg:hidden sticky top-0 z-40 flex items-center h-12 px-3 border-b border-border bg-sidebar/95 backdrop-blur">
      <div className="flex items-center gap-2 flex-1 font-bold text-sm text-primary tracking-widest uppercase">
        <Terminal className="w-4 h-4" />
        Quant Terminal
      </div>
      <Sheet open={open} onOpenChange={setOpen}>
        <SheetTrigger asChild>
          <button
            className="p-2 rounded-md text-muted-foreground hover:text-foreground hover:bg-muted/50 transition-colors"
            aria-label="Open navigation"
          >
            {open ? <X className="w-5 h-5" /> : <Menu className="w-5 h-5" />}
          </button>
        </SheetTrigger>
        <SheetContent side="left" className="w-64 p-0 bg-sidebar border-border">
          <div className="h-12 flex items-center gap-2 px-4 border-b border-border font-bold text-sm text-primary tracking-widest uppercase">
            <Terminal className="w-4 h-4" />
            Quant Terminal
          </div>
          <NavLinks onNavigate={() => setOpen(false)} />
        </SheetContent>
      </Sheet>
    </header>
  );
}

export function Layout({ children }: { children: ReactNode }) {
  return (
    <div className="flex h-screen bg-background text-foreground overflow-hidden">
      <DesktopSidebar />
      <div className="flex-1 flex flex-col min-w-0 overflow-hidden">
        <MobileHeader />
        <main className="flex-1 overflow-auto min-h-0">
          {children}
        </main>
      </div>
    </div>
  );
}
