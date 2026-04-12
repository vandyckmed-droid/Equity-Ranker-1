import { ReactNode, createContext, useCallback, useContext, useMemo, useState } from "react";
import { Link, useLocation } from "wouter";
import { cn } from "@/lib/utils";
import { BarChart3, LineChart, Terminal } from "lucide-react";
import { usePortfolio } from "@/hooks/use-portfolio";

const TABS = [
  { href: "/", label: "Rankings", icon: BarChart3 },
  { href: "/portfolio", label: "Portfolio", icon: LineChart },
] as const;

type BottomActionsCtx = { setActions: (node: ReactNode) => void };
const BottomActionsContext = createContext<BottomActionsCtx | null>(null);
export function useBottomActions() { return useContext(BottomActionsContext); }

function MobileTabBar({ actions }: { actions: ReactNode }) {
  const [location] = useLocation();
  const { basket } = usePortfolio();

  return (
    <nav
      className="lg:hidden fixed bottom-0 inset-x-0 z-50 flex border-t border-border bg-sidebar/95 backdrop-blur-md"
      style={{ paddingBottom: "env(safe-area-inset-bottom)" }}
    >
      {TABS.map(({ href, label, icon: Icon }) => {
        const active = location === href;
        const showBadge = href === "/portfolio" && basket.length > 0;
        return (
          <Link
            key={href}
            href={href}
            className={cn(
              "flex-1 flex flex-col items-center justify-center gap-0.5 py-2.5 transition-colors touch-manipulation select-none",
              active ? "text-primary" : "text-muted-foreground/60 hover:text-muted-foreground"
            )}
          >
            <div className="relative">
              <Icon className={cn("w-5 h-5 transition-transform", active && "scale-110")} />
              {showBadge && (
                <span className="absolute -top-1 -right-2 bg-primary text-primary-foreground text-[9px] font-bold min-w-[15px] h-[15px] rounded-full flex items-center justify-center px-0.5 leading-none">
                  {basket.length > 9 ? "9+" : basket.length}
                </span>
              )}
            </div>
            <span className={cn("text-[10px] tracking-wide", active ? "font-semibold" : "font-medium")}>
              {label}
            </span>
          </Link>
        );
      })}
      {actions && (
        <div className="flex items-stretch border-l border-border/40">
          {actions}
        </div>
      )}
    </nav>
  );
}

function DesktopSidebar() {
  const [location] = useLocation();
  const { basket } = usePortfolio();

  const NAV = [
    { href: "/", label: "Universe Rankings", icon: BarChart3 },
    { href: "/portfolio", label: "Portfolio & Risk", icon: LineChart },
  ] as const;

  return (
    <aside className="hidden lg:flex w-56 border-r border-border bg-sidebar flex-col h-screen shrink-0 sticky top-0">
      <div className="h-12 flex items-center gap-2 px-4 border-b border-border font-bold text-sm text-primary tracking-widest uppercase">
        <Terminal className="w-4 h-4" />
        Quant Terminal
      </div>
      <nav className="flex-1 overflow-y-auto">
        <div className="flex flex-col gap-1 p-3">
          {NAV.map(({ href, label, icon: Icon }) => {
            const active = location === href;
            const showBadge = href === "/portfolio" && basket.length > 0;
            return (
              <Link
                key={href}
                href={href}
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
        </div>
      </nav>
      <div className="p-4 text-xs text-muted-foreground border-t border-border">
        v1.0.0-beta
      </div>
    </aside>
  );
}

function MobileHeader() {
  return (
    <header className="lg:hidden sticky top-0 z-40 flex items-center h-12 px-4 border-b border-border bg-sidebar/95 backdrop-blur">
      <div className="flex items-center gap-2 font-bold text-sm text-primary tracking-widest uppercase">
        <Terminal className="w-4 h-4" />
        Quant Terminal
      </div>
    </header>
  );
}

export function Layout({ children }: { children: ReactNode }) {
  const [actions, setActionsState] = useState<ReactNode>(null);
  const setActions = useCallback((node: ReactNode) => setActionsState(node), []);
  const ctx = useMemo(() => ({ setActions }), [setActions]);
  return (
    <BottomActionsContext.Provider value={ctx}>
      <div className="flex h-screen bg-background text-foreground overflow-hidden">
        <DesktopSidebar />
        <div className="flex-1 flex flex-col min-w-0 overflow-hidden">
          <MobileHeader />
          <main className="flex-1 overflow-auto min-h-0 pb-16 lg:pb-0">
            {children}
          </main>
          <MobileTabBar actions={actions} />
        </div>
      </div>
    </BottomActionsContext.Provider>
  );
}
