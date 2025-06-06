"use client";

import Link from "next/link";
import { Button } from "@/components/ui/button";
import { 
  NavigationMenu,
  NavigationMenuContent,
  NavigationMenuItem,
  NavigationMenuLink,
  NavigationMenuList,
  NavigationMenuTrigger,
} from "@/components/ui/navigation-menu";
import { Badge } from "@/components/ui/badge";
import { Trophy, Users, BarChart3, Calendar, Swords, Target } from "lucide-react";

export function SiteHeader() {
  return (
    <header className="sticky top-0 z-50 w-full border-b bg-background/95 backdrop-blur supports-[backdrop-filter]:bg-background/60">
      <div className="container flex h-16 items-center justify-between">
        <Link href="/" className="flex items-center space-x-2">
          <Trophy className="h-6 w-6 text-primary" />
          <span className="font-bold text-xl">The League</span>
          <Badge variant="secondary" className="text-xs">20+ Years</Badge>
        </Link>

        <NavigationMenu>
          <NavigationMenuList>
            <NavigationMenuItem>
              <NavigationMenuTrigger>League</NavigationMenuTrigger>
              <NavigationMenuContent>
                <div className="grid gap-3 p-6 w-[400px]">
                  <NavigationMenuLink asChild>
                    <Link
                      href="/dashboard"
                      className="flex items-center space-x-3 rounded-md p-3 hover:bg-accent"
                    >
                      <BarChart3 className="h-5 w-5" />
                      <div>
                        <div className="font-medium">Dashboard</div>
                        <div className="text-sm text-muted-foreground">
                          Live standings and recent activity
                        </div>
                      </div>
                    </Link>
                  </NavigationMenuLink>
                  <NavigationMenuLink asChild>
                    <Link
                      href="/owners"
                      className="flex items-center space-x-3 rounded-md p-3 hover:bg-accent"
                    >
                      <Users className="h-5 w-5" />
                      <div>
                        <div className="font-medium">Owners</div>
                        <div className="text-sm text-muted-foreground">
                          Individual profiles and stats
                        </div>
                      </div>
                    </Link>
                  </NavigationMenuLink>
                </div>
              </NavigationMenuContent>
            </NavigationMenuItem>

            <NavigationMenuItem>
              <NavigationMenuTrigger>History</NavigationMenuTrigger>
              <NavigationMenuContent>
                <div className="grid gap-3 p-6 w-[400px]">
                  <NavigationMenuLink asChild>
                    <Link
                      href="/seasons"
                      className="flex items-center space-x-3 rounded-md p-3 hover:bg-accent"
                    >
                      <Calendar className="h-5 w-5" />
                      <div>
                        <div className="font-medium">Seasons Archive</div>
                        <div className="text-sm text-muted-foreground">
                          Historical data from 2004-2025
                        </div>
                      </div>
                    </Link>
                  </NavigationMenuLink>
                  <NavigationMenuLink asChild>
                    <Link
                      href="/head-to-head"
                      className="flex items-center space-x-3 rounded-md p-3 hover:bg-accent"
                    >
                      <Swords className="h-5 w-5" />
                      <div>
                        <div className="font-medium">Head-to-Head</div>
                        <div className="text-sm text-muted-foreground">
                          Matchup analysis and records
                        </div>
                      </div>
                    </Link>
                  </NavigationMenuLink>
                  <NavigationMenuLink asChild>
                    <Link
                      href="/draft"
                      className="flex items-center space-x-3 rounded-md p-3 hover:bg-accent"
                    >
                      <Target className="h-5 w-5" />
                      <div>
                        <div className="font-medium">Draft Central</div>
                        <div className="text-sm text-muted-foreground">
                          Draft history and analysis
                        </div>
                      </div>
                    </Link>
                  </NavigationMenuLink>
                </div>
              </NavigationMenuContent>
            </NavigationMenuItem>

            <NavigationMenuItem>
              <Link href="/analytics" legacyBehavior passHref>
                <NavigationMenuLink className="group inline-flex h-10 w-max items-center justify-center rounded-md bg-background px-4 py-2 text-sm font-medium transition-colors hover:bg-accent hover:text-accent-foreground focus:bg-accent focus:text-accent-foreground focus:outline-none disabled:pointer-events-none disabled:opacity-50">
                  Analytics Lab
                </NavigationMenuLink>
              </Link>
            </NavigationMenuItem>
          </NavigationMenuList>
        </NavigationMenu>

        <div className="flex items-center space-x-2">
          <Button variant="outline" size="sm">
            2025 Season
          </Button>
        </div>
      </div>
    </header>
  );
} 