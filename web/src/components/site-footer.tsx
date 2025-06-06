import { Trophy } from "lucide-react";
import { Separator } from "@/components/ui/separator";

export function SiteFooter() {
  return (
    <footer className="border-t bg-background">
      <div className="container py-12">
        <div className="grid grid-cols-1 md:grid-cols-4 gap-8">
          {/* Logo & Description */}
          <div className="md:col-span-2">
            <div className="flex items-center space-x-2 mb-4">
              <Trophy className="h-6 w-6 text-primary" />
              <span className="font-bold text-xl">The League</span>
            </div>
            <p className="text-muted-foreground mb-4 max-w-md">
              Twenty-two years of fantasy football excellence. Celebrating the stats, 
              stories, and legends that make our league legendary.
            </p>
            <p className="text-sm text-muted-foreground">
              Est. 2004 • 10 Dedicated Owners • Endless Memories
            </p>
          </div>

          {/* Quick Links */}
          <div>
            <h3 className="font-semibold mb-4">Explore</h3>
            <ul className="space-y-2 text-sm text-muted-foreground">
              <li><a href="/dashboard" className="hover:text-foreground transition-colors">Dashboard</a></li>
              <li><a href="/owners" className="hover:text-foreground transition-colors">Owners</a></li>
              <li><a href="/seasons" className="hover:text-foreground transition-colors">Seasons</a></li>
              <li><a href="/analytics" className="hover:text-foreground transition-colors">Analytics</a></li>
            </ul>
          </div>

          {/* History */}
          <div>
            <h3 className="font-semibold mb-4">History</h3>
            <ul className="space-y-2 text-sm text-muted-foreground">
              <li><a href="/head-to-head" className="hover:text-foreground transition-colors">Head-to-Head</a></li>
              <li><a href="/draft" className="hover:text-foreground transition-colors">Draft Central</a></li>
              <li><a href="/championships" className="hover:text-foreground transition-colors">Championships</a></li>
              <li><a href="/records" className="hover:text-foreground transition-colors">League Records</a></li>
            </ul>
          </div>
        </div>

        <Separator className="my-8" />

        <div className="flex flex-col md:flex-row justify-between items-center text-sm text-muted-foreground">
          <p>© 2025 The League. Built with pride and Next.js.</p>
          <p>
            Data spans 2004-2025 • 
            <span className="text-primary font-medium"> Privacy-First Design</span>
          </p>
        </div>
      </div>
    </footer>
  );
} 