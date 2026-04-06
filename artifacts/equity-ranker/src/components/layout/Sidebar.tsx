import { Link, useLocation } from "wouter";
import { cn } from "@/lib/utils";
import { BarChart3, Calculator, LineChart } from "lucide-react";
import { usePortfolio } from "@/hooks/use-portfolio";

export function Sidebar() {
  const [location] = useLocation();
  const { holdings } = usePortfolio();

  return (
    <aside className="w-64 border-r border-border bg-sidebar flex flex-col h-screen shrink-0 sticky top-0">
      <div className="h-14 flex items-center px-4 border-b border-border font-bold text-lg text-primary tracking-tight">
        QUANT TERMINAL
      </div>
      <nav className="flex-1 p-4 space-y-1">
        <Link
          href="/"
          className={cn(
            "flex items-center gap-3 px-3 py-2 text-sm rounded-md transition-colors",
            location === "/"
              ? "bg-sidebar-accent text-sidebar-accent-foreground font-medium"
              : "text-sidebar-foreground/70 hover:bg-sidebar-accent/50 hover:text-sidebar-accent-foreground"
          )}
        >
          <BarChart3 className="w-4 h-4" />
          Universe Rankings
        </Link>
        <Link
          href="/portfolio"
          className={cn(
            "flex items-center gap-3 px-3 py-2 text-sm rounded-md transition-colors",
            location === "/portfolio"
              ? "bg-sidebar-accent text-sidebar-accent-foreground font-medium"
              : "text-sidebar-foreground/70 hover:bg-sidebar-accent/50 hover:text-sidebar-accent-foreground"
          )}
        >
          <LineChart className="w-4 h-4" />
          Portfolio & Risk
          {holdings.length > 0 && (
            <span className="ml-auto bg-primary/20 text-primary text-[10px] font-bold px-2 py-0.5 rounded-full">
              {holdings.length}
            </span>
          )}
        </Link>
        <Link
          href="/methodology"
          className={cn(
            "flex items-center gap-3 px-3 py-2 text-sm rounded-md transition-colors",
            location === "/methodology"
              ? "bg-sidebar-accent text-sidebar-accent-foreground font-medium"
              : "text-sidebar-foreground/70 hover:bg-sidebar-accent/50 hover:text-sidebar-accent-foreground"
          )}
        >
          <Calculator className="w-4 h-4" />
          Methodology
        </Link>
      </nav>
      <div className="p-4 text-xs text-muted-foreground border-t border-border">
        v1.0.0-beta
      </div>
    </aside>
  );
}
