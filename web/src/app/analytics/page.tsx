"use client";

import { useState, useEffect } from "react";
import { motion } from "framer-motion";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { 
  LineChart, 
  Line, 
  BarChart, 
  Bar, 
  XAxis, 
  YAxis, 
  CartesianGrid, 
  Tooltip, 
  Legend, 
  ResponsiveContainer,
  PieChart,
  Pie,
  Cell
} from "recharts";
import { 
  TrendingUp, 
  BarChart3, 
  PieChart as PieChartIcon,
  Target,
  Trophy,
  Clock,
  Users,
  Zap,
  Loader2
} from "lucide-react";
import { AnalyticsData } from "@/lib/types";

export default function Analytics() {
  const [data, setData] = useState<AnalyticsData | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    fetchAnalyticsData();
  }, []);

  const fetchAnalyticsData = async () => {
    try {
      setLoading(true);
      const response = await fetch('/api/analytics');
      const result = await response.json();
      
      if (result.success) {
        setData(result.data);
      } else {
        setError(result.error || 'Failed to fetch data');
      }
    } catch (err) {
      setError('Failed to connect to API');
      console.error('Analytics fetch error:', err);
    } finally {
      setLoading(false);
    }
  };

  if (loading) {
    return (
      <div className="container py-8 flex items-center justify-center min-h-[400px]">
        <div className="flex items-center space-x-2">
          <Loader2 className="w-6 h-6 animate-spin" />
          <span>Loading analytics...</span>
        </div>
      </div>
    );
  }

  if (error || !data) {
    return (
      <div className="container py-8 flex items-center justify-center min-h-[400px]">
        <Card className="p-6 text-center">
          <CardHeader>
            <CardTitle className="text-red-600">Error Loading Analytics</CardTitle>
            <CardDescription>{error || 'No data available'}</CardDescription>
          </CardHeader>
          <CardContent>
            <Button onClick={fetchAnalyticsData}>
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
            <h1 className="text-3xl md:text-4xl font-bold mb-2">Analytics Lab</h1>
            <p className="text-muted-foreground text-lg">
              Deep dive into {data.metrics.totalSeasons} years of fantasy football data
            </p>
          </div>
          <div className="flex items-center space-x-2">
            <Badge variant="outline" className="bg-blue-50 text-blue-700 border-blue-200">
              <BarChart3 className="w-3 h-3 mr-1" />
              {data.metrics.totalGames}+ Games
            </Badge>
            <Button>
              <Target className="w-4 h-4 mr-2" />
              Export Data
            </Button>
          </div>
        </div>
      </motion.div>

      {/* Key Metrics */}
      <motion.div
        initial={{ opacity: 0, y: 20 }}
        animate={{ opacity: 1, y: 0 }}
        transition={{ duration: 0.6, delay: 0.1 }}
      >
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4 mb-8">
          <Card>
            <CardHeader className="pb-3">
              <div className="flex items-center justify-between">
                <Trophy className="w-8 h-8 text-yellow-500" />
                <Badge variant="secondary">All Time</Badge>
              </div>
            </CardHeader>
            <CardContent>
              <div className="text-2xl font-bold">{data.metrics.totalPoints.toLocaleString()}</div>
              <p className="text-sm text-muted-foreground">Total Points Scored</p>
            </CardContent>
          </Card>
          
          <Card>
            <CardHeader className="pb-3">
              <div className="flex items-center justify-between">
                <Users className="w-8 h-8 text-blue-500" />
                <Badge variant="secondary">Active</Badge>
              </div>
            </CardHeader>
            <CardContent>
              <div className="text-2xl font-bold">{data.metrics.totalTrades.toLocaleString()}</div>
              <p className="text-sm text-muted-foreground">Total Trades</p>
            </CardContent>
          </Card>

          <Card>
            <CardHeader className="pb-3">
              <div className="flex items-center justify-between">
                <Zap className="w-8 h-8 text-green-500" />
                <Badge variant="secondary">Weekly</Badge>
              </div>
            </CardHeader>
            <CardContent>
              <div className="text-2xl font-bold">{data.metrics.totalWaivers.toLocaleString()}</div>
              <p className="text-sm text-muted-foreground">Waiver Claims</p>
            </CardContent>
          </Card>

          <Card>
            <CardHeader className="pb-3">
              <div className="flex items-center justify-between">
                <Clock className="w-8 h-8 text-purple-500" />
                <Badge variant="secondary">Record</Badge>
              </div>
            </CardHeader>
            <CardContent>
              <div className="text-2xl font-bold">{data.metrics.highestScore}</div>
              <p className="text-sm text-muted-foreground">Highest Week Score</p>
            </CardContent>
          </Card>
        </div>
      </motion.div>

      {/* Charts Section */}
      <motion.div
        initial={{ opacity: 0, y: 20 }}
        animate={{ opacity: 1, y: 0 }}
        transition={{ duration: 0.6, delay: 0.2 }}
      >
        <Card>
          <CardHeader>
            <CardTitle className="flex items-center space-x-2">
              <TrendingUp className="w-5 h-5" />
              <span>Seasonal Performance Trends</span>
            </CardTitle>
            <CardDescription>
              Points scored by top performers over recent seasons
            </CardDescription>
          </CardHeader>
          <CardContent>
            <ResponsiveContainer width="100%" height={300}>
              <LineChart data={data.seasonalData}>
                <CartesianGrid strokeDasharray="3 3" />
                <XAxis dataKey="season" />
                <YAxis />
                <Tooltip />
                <Legend />
                {/* Dynamically generate lines based on available manager data */}
                {data.seasonalData.length > 0 && Object.keys(data.seasonalData[0])
                  .filter(key => key !== 'season')
                  .slice(0, 4)
                  .map((key, index) => {
                    const colors = ['#8884d8', '#82ca9d', '#ffc658', '#ff7300'];
                    const managerName = key.replace('Points', '').replace(/([a-z])([A-Z])/g, '$1 $2');
                    return (
                      <Line 
                        key={key}
                        type="monotone" 
                        dataKey={key} 
                        stroke={colors[index]} 
                        strokeWidth={3} 
                        name={managerName} 
                      />
                    );
                  })
                }
              </LineChart>
            </ResponsiveContainer>
          </CardContent>
        </Card>

        <div className="grid lg:grid-cols-2 gap-6 mt-6">
          <Card>
            <CardHeader>
              <CardTitle className="flex items-center space-x-2">
                <BarChart3 className="w-5 h-5" />
                <span>Owner Performance</span>
              </CardTitle>
              <CardDescription>
                Average points and success rates
              </CardDescription>
            </CardHeader>
            <CardContent>
              <ResponsiveContainer width="100%" height={300}>
                <BarChart data={data.ownerPerformance}>
                  <CartesianGrid strokeDasharray="3 3" />
                  <XAxis dataKey="owner" />
                  <YAxis />
                  <Tooltip />
                  <Legend />
                  <Bar dataKey="avgPoints" fill="#8884d8" name="Avg Points" />
                  <Bar dataKey="playoffRate" fill="#82ca9d" name="Playoff %" />
                </BarChart>
              </ResponsiveContainer>
            </CardContent>
          </Card>

          <Card>
            <CardHeader>
              <CardTitle className="flex items-center space-x-2">
                <PieChartIcon className="w-5 h-5" />
                <span>Draft Position Distribution</span>
              </CardTitle>
              <CardDescription>
                Player positions drafted across all seasons
              </CardDescription>
            </CardHeader>
            <CardContent>
              <ResponsiveContainer width="100%" height={300}>
                <PieChart>
                  <Pie
                    data={data.positionBreakdown}
                    cx="50%"
                    cy="50%"
                    labelLine={false}
                    label={({ position, percent }) => `${position} ${(percent * 100).toFixed(0)}%`}
                    outerRadius={80}
                    fill="#8884d8"
                    dataKey="value"
                  >
                    {data.positionBreakdown.map((entry, index) => (
                      <Cell key={`cell-${index}`} fill={entry.color} />
                    ))}
                  </Pie>
                  <Tooltip />
                </PieChart>
              </ResponsiveContainer>
            </CardContent>
          </Card>
        </div>
      </motion.div>
    </div>
  );
} 