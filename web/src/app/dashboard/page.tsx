"use client";

import { useState, useEffect } from "react";
import { motion } from "framer-motion";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Avatar, AvatarFallback } from "@/components/ui/avatar";
import { 
  Trophy, 
  TrendingUp, 
  Users, 
  Target,
  Calendar,
  Clock,
  Star,
  Award,
  Loader2
} from "lucide-react";
import { DashboardData } from "@/lib/types";

const iconMap = {
  trade: Target,
  waiver: Users,
  score: TrendingUp,
  milestone: Star,
};

export default function Dashboard() {
  const [data, setData] = useState<DashboardData | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    fetchDashboardData();
  }, []);

  const fetchDashboardData = async () => {
    try {
      setLoading(true);
      const response = await fetch('/api/dashboard');
      const result = await response.json();
      
      if (result.success) {
        setData(result.data);
      } else {
        setError(result.error || 'Failed to fetch data');
      }
    } catch (err) {
      setError('Failed to connect to API');
      console.error('Dashboard fetch error:', err);
    } finally {
      setLoading(false);
    }
  };

  if (loading) {
    return (
      <div className="container py-8 flex items-center justify-center min-h-[400px]">
        <div className="flex items-center space-x-2">
          <Loader2 className="w-6 h-6 animate-spin" />
          <span>Loading dashboard...</span>
        </div>
      </div>
    );
  }

  if (error || !data) {
    return (
      <div className="container py-8 flex items-center justify-center min-h-[400px]">
        <Card className="p-6 text-center">
          <CardHeader>
            <CardTitle className="text-red-600">Error Loading Dashboard</CardTitle>
            <CardDescription>{error || 'No data available'}</CardDescription>
          </CardHeader>
          <CardContent>
            <Button onClick={fetchDashboardData}>
              Try Again
            </Button>
          </CardContent>
        </Card>
      </div>
    );
  }

  return (
    <div className="container py-8 space-y-8">
      {/* Page Header */}
      <motion.div
        initial={{ opacity: 0, y: 20 }}
        animate={{ opacity: 1, y: 0 }}
        transition={{ duration: 0.6 }}
      >
        <div className="flex flex-col md:flex-row md:items-center justify-between mb-8">
          <div>
            <h1 className="text-3xl md:text-4xl font-bold mb-2">League Dashboard</h1>
            <p className="text-muted-foreground text-lg">
              {data.currentLeague.season} Season • Week {data.currentLeague.current_week || 1} • {data.currentLeague.name}
            </p>
          </div>
          <div className="flex items-center space-x-2">
            <Badge variant="outline" className="bg-green-50 text-green-700 border-green-200">
              <Clock className="w-3 h-3 mr-1" />
              Live Scoring
            </Badge>
            <Button>
              <Calendar className="w-4 h-4 mr-2" />
              View Schedule
            </Button>
          </div>
        </div>
      </motion.div>

      {/* Weekly Highlights */}
      <motion.div
        initial={{ opacity: 0, y: 20 }}
        animate={{ opacity: 1, y: 0 }}
        transition={{ duration: 0.6, delay: 0.1 }}
      >
        <h2 className="text-2xl font-bold mb-6">This Week&apos;s Highlights</h2>
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
                      {data.weeklyHighlights.map((highlight) => (
            <Card key={highlight.title} className="relative overflow-hidden group hover:shadow-lg transition-shadow">
              <div className={`absolute inset-0 bg-gradient-to-br ${highlight.color} opacity-5 group-hover:opacity-10 transition-opacity`} />
              <CardHeader className="pb-3">
                <CardDescription className="font-medium">{highlight.title}</CardDescription>
                <CardTitle className="text-xl">{highlight.value}</CardTitle>
              </CardHeader>
              <CardContent>
                <p className="text-sm text-muted-foreground">{highlight.subtitle}</p>
              </CardContent>
            </Card>
          ))}
        </div>
      </motion.div>

      <div className="grid lg:grid-cols-3 gap-8">
        {/* League Standings */}
        <motion.div
          className="lg:col-span-2"
          initial={{ opacity: 0, x: -20 }}
          animate={{ opacity: 1, x: 0 }}
          transition={{ duration: 0.6, delay: 0.2 }}
        >
          <Card>
            <CardHeader>
              <CardTitle className="flex items-center space-x-2">
                <Trophy className="w-5 h-5 text-yellow-500" />
                <span>Current Standings</span>
              </CardTitle>
              <CardDescription>
                Regular season complete • Playoffs in progress
              </CardDescription>
            </CardHeader>
            <CardContent className="space-y-3">
              {data.standings.map((team, index) => (
                <motion.div
                  key={team.owner}
                  className={`flex items-center justify-between p-3 rounded-lg border transition-colors ${
                    team.isChampion ? 'bg-yellow-50 border-yellow-200 dark:bg-yellow-900/20' : 'hover:bg-muted/50'
                  }`}
                  initial={{ opacity: 0, x: -10 }}
                  animate={{ opacity: 1, x: 0 }}
                  transition={{ duration: 0.3, delay: index * 0.05 }}
                >
                  <div className="flex items-center space-x-3">
                    <div className={`w-8 h-8 rounded-full flex items-center justify-center text-sm font-bold ${
                      team.rank <= 3 ? 'bg-gradient-to-r from-yellow-400 to-orange-500 text-white' : 'bg-muted'
                    }`}>
                      {team.rank}
                    </div>
                    <Avatar className="w-10 h-10">
                      <AvatarFallback>{team.owner.split(' ')[0][0]}{team.owner.split(' ')[1]?.[0] || ''}</AvatarFallback>
                    </Avatar>
                    <div>
                      <div className="font-medium flex items-center space-x-2">
                        <span>{team.owner}</span>
                        {team.isChampion && <Award className="w-4 h-4 text-yellow-500" />}
                      </div>
                      <div className="text-sm text-muted-foreground">
                        {team.wins}-{team.losses} • {team.points.toFixed(1)} pts
                      </div>
                    </div>
                  </div>
                  <div className="text-right">
                    <Badge variant={team.streak.startsWith('W') ? 'default' : 'secondary'} className="text-xs">
                      {team.streak}
                    </Badge>
                  </div>
                </motion.div>
              ))}
            </CardContent>
          </Card>
        </motion.div>

        {/* Recent Activity */}
        <motion.div
          className="space-y-6"
          initial={{ opacity: 0, x: 20 }}
          animate={{ opacity: 1, x: 0 }}
          transition={{ duration: 0.6, delay: 0.3 }}
        >
          <Card>
            <CardHeader>
              <CardTitle className="flex items-center space-x-2">
                <Clock className="w-5 h-5" />
                <span>Recent Activity</span>
              </CardTitle>
            </CardHeader>
            <CardContent className="space-y-4">
              {data.recentActivity.map((activity, index) => {
                const IconComponent = iconMap[activity.type as keyof typeof iconMap] || Target;
                return (
                  <div key={index} className="flex items-start space-x-3">
                    <div className="p-2 rounded-full bg-primary/10">
                      <IconComponent className="w-4 h-4 text-primary" />
                    </div>
                    <div className="flex-1 min-w-0">
                      <p className="text-sm font-medium">{activity.description}</p>
                      <p className="text-xs text-muted-foreground">{activity.time}</p>
                    </div>
                  </div>
                );
              })}
            </CardContent>
          </Card>

          <Card>
            <CardHeader>
              <CardTitle>Playoff Picture</CardTitle>
              <CardDescription>Championship bracket</CardDescription>
            </CardHeader>
            <CardContent>
              <div className="space-y-3">
                <div className="flex justify-between items-center p-2 bg-green-50 dark:bg-green-900/20 rounded">
                  <span className="text-sm font-medium">Championship</span>
                  <Badge variant="outline">Week 15</Badge>
                </div>
                <div className="pl-4 space-y-1">
                  <div className="text-sm">Top seeds compete for championship</div>
                </div>
                <div className="flex justify-between items-center p-2 bg-orange-50 dark:bg-orange-900/20 rounded">
                  <span className="text-sm font-medium">Consolation</span>
                  <Badge variant="outline">Week 15</Badge>
                </div>
                <div className="pl-4 space-y-1">
                  <div className="text-sm">Lower seeds compete for pride</div>
                </div>
              </div>
            </CardContent>
          </Card>
        </motion.div>
      </div>
    </div>
  );
} 