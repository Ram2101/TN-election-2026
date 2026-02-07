'use client'

import { useEffect, useState, useMemo } from 'react'
import dynamic from 'next/dynamic'
import { 
  ConstituencyWinner, 
  fetchConstituencyWinners, 
  fetchAllianceSummary,
  AllianceSummary,
  ALLIANCE_COLORS,
  getAllianceColor,
  isSupabaseConfigured
} from '@/lib/supabase'

// Loading component (defined before use)
function MapLoading() {
  return (
    <div className="h-full w-full bg-bg-card rounded-xl flex items-center justify-center">
      <div className="text-gray-400 animate-pulse">Loading map...</div>
    </div>
  )
}

// Dynamically import map component (required for Leaflet SSR issues)
const ElectionMap = dynamic(
  () => import('@/components/ElectionMap'),
  { ssr: false, loading: () => <MapLoading /> }
)
const MapLegend = dynamic(
  () => import('@/components/ElectionMap').then(mod => mod.MapLegend),
  { ssr: false }
)

export default function Home() {
  const [winners, setWinners] = useState<ConstituencyWinner[]>([])
  const [allianceSummary, setAllianceSummary] = useState<AllianceSummary[]>([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [selectedAlliance, setSelectedAlliance] = useState<string | null>(null)
  const [selectedConstituency, setSelectedConstituency] = useState<ConstituencyWinner | null>(null)

  useEffect(() => {
    // Check for environment variables
    if (!isSupabaseConfigured()) {
      setError('Missing Supabase credentials. Please create .env.local with NEXT_PUBLIC_SUPABASE_URL and NEXT_PUBLIC_SUPABASE_ANON_KEY')
      setLoading(false)
      return
    }

    async function loadData() {
      try {
        // Fetch both winners and alliance summary in parallel
        const [winnersData, summaryData] = await Promise.all([
          fetchConstituencyWinners(),
          fetchAllianceSummary()
        ])
        
        if (winnersData.length === 0) {
          setError('No predictions found. Make sure you have run the backend processor to generate predictions.')
        } else {
          setWinners(winnersData)
          setAllianceSummary(summaryData)
          setError(null) // Clear any previous errors
        }
      } catch (err: any) {
        // Extract detailed error message
        let errorMessage = 'Failed to load predictions'
        
        if (err?.message) {
          errorMessage = err.message
        } else if (err?.error?.message) {
          errorMessage = err.error.message
        } else if (typeof err === 'string') {
          errorMessage = err
        }
        
        // Check for common Supabase errors
        if (errorMessage.includes('relation') && errorMessage.includes('does not exist')) {
          errorMessage = 'Database view not found. Please run schema.sql in Supabase SQL Editor.'
        } else if (errorMessage.includes('JWT') || errorMessage.includes('token')) {
          errorMessage = 'Invalid Supabase credentials. Check your .env.local file.'
        } else if (errorMessage.includes('fetch')) {
          errorMessage = 'Network error. Check your internet connection and Supabase URL.'
        }
        
        setError(errorMessage)
        console.error('Failed to fetch data:', err)
        console.error('Full error object:', JSON.stringify(err, null, 2))
      } finally {
        setLoading(false)
      }
    }
    
    loadData()
    
    // Refresh every 5 minutes
    const interval = setInterval(loadData, 5 * 60 * 1000)
    return () => clearInterval(interval)
  }, [])

  // Validate and deduplicate winners (ensure exactly one per constituency)
  const uniqueWinners = useMemo(() => {
    const seen = new Set<string>()
    const unique: ConstituencyWinner[] = []
    
    for (const winner of winners) {
      const key = winner.constituency_name.toUpperCase().trim()
      if (!seen.has(key)) {
        seen.add(key)
        unique.push(winner)
      }
    }
    
    return unique
  }, [winners])

  // Get unique districts (should be 38)
  const uniqueDistricts = useMemo(() => {
    return new Set(uniqueWinners.map(w => w.district)).size
  }, [uniqueWinners])

  // Valid alliances (from config) - filter out invalid/duplicate alliances
  const VALID_ALLIANCES = ['DMK_Front', 'ADMK_Front', 'TVK_Front', 'NTK']

  // Create alliance summary map from fetched data (from ALL predictions, not just winners)
  const allianceSummaryMap = useMemo(() => {
    const map: Record<string, { count: number; totalSentiment: number; totalSources: number }> = {}
    
    // Initialize with data from v_alliance_summary (accurate source counts)
    allianceSummary.forEach(summary => {
      if (VALID_ALLIANCES.includes(summary.alliance)) {
        map[summary.alliance] = {
          count: 0, // Will be filled from winners
          totalSentiment: summary.avg_sentiment * (summary.constituency_count || 0),
          totalSources: summary.total_sources || 0
        }
      }
    })
    
    // Count seats from winners (for seat count)
    uniqueWinners.forEach(w => {
      if (VALID_ALLIANCES.includes(w.predicted_alliance)) {
        if (!map[w.predicted_alliance]) {
          map[w.predicted_alliance] = { count: 0, totalSentiment: 0, totalSources: 0 }
        }
        map[w.predicted_alliance].count++
      }
    })
    
    // Ensure all valid alliances are shown (even with 0 seats) for consistent UI
    VALID_ALLIANCES.forEach(alliance => {
      if (!map[alliance]) {
        map[alliance] = { count: 0, totalSentiment: 0, totalSources: 0 }
      }
    })
    
    return map
  }, [allianceSummary, uniqueWinners])

  return (
    <main className="min-h-screen p-4 lg:p-8">
      {/* Header */}
      <header className="mb-6">
        <div className="flex items-center justify-between">
          <div>
            <h1 className="text-3xl lg:text-4xl font-bold bg-gradient-to-r from-blue-400 to-purple-500 bg-clip-text text-transparent">
              PollPulse TN
            </h1>
            <p className="text-gray-400 mt-1">
              2026 Tamil Nadu Election Sentiment Analysis
            </p>
          </div>
          <div className="text-right">
            <div className="text-sm text-gray-400">Last Updated</div>
            <div className="text-white font-mono">
              {new Date().toLocaleDateString('en-IN', { 
                day: 'numeric', 
                month: 'short', 
                year: 'numeric',
                hour: '2-digit',
                minute: '2-digit'
              })}
            </div>
          </div>
        </div>
      </header>

      {/* Main Grid */}
      <div className="grid grid-cols-1 lg:grid-cols-4 gap-6">
        {/* Sidebar - Alliance Filter */}
        <div className="lg:col-span-1 space-y-4">
          {/* Alliance Cards */}
          <div className="bg-bg-card rounded-xl p-4">
            <h2 className="text-lg font-semibold text-white mb-4">Alliances</h2>
            
            <button
              onClick={() => setSelectedAlliance(null)}
              className={`w-full mb-2 px-4 py-3 rounded-lg text-left transition-all ${
                selectedAlliance === null 
                  ? 'bg-accent text-white' 
                  : 'bg-gray-700 text-gray-300 hover:bg-gray-600'
              }`}
            >
              <div className="font-medium">All Alliances</div>
              <div className="text-sm opacity-75">{uniqueWinners.length} constituencies</div>
            </button>

            <div className="space-y-2">
              {Object.entries(allianceSummaryMap)
                .filter(([alliance]) => VALID_ALLIANCES.includes(alliance))  // Only show valid alliances
                .sort((a, b) => b[1].count - a[1].count)
                .map(([alliance, data]) => {
                  // Get constituency count from alliance summary for accurate average
                  const summaryItem = allianceSummary.find(s => s.alliance === alliance)
                  const constituencyCount = summaryItem?.constituency_count || 1
                  const avgSentiment = constituencyCount > 0 
                    ? (summaryItem?.avg_sentiment || 0) * 100
                    : 0
                  
                  return (
                    <button
                      key={alliance}
                      onClick={() => setSelectedAlliance(selectedAlliance === alliance ? null : alliance)}
                      className={`w-full px-4 py-3 rounded-lg text-left transition-all ${
                        selectedAlliance === alliance 
                          ? 'ring-2 ring-white' 
                          : 'hover:opacity-80'
                      }`}
                      style={{ 
                        backgroundColor: getAllianceColor(alliance),
                        color: alliance.includes('TVK') ? '#000' : '#fff'
                      }}
                    >
                      <div className="font-medium">{alliance.replace('_', ' ')}</div>
                      <div className="text-sm opacity-75">
                        {data.count} seats | {data.totalSources} sources
                      </div>
                      <div className="text-sm mt-1">
                        Avg Sentiment: {avgSentiment.toFixed(1)}%
                      </div>
                    </button>
                  )
                })}
            </div>
          </div>

          {/* Stats Card */}
          <div className="bg-bg-card rounded-xl p-4">
            <h2 className="text-lg font-semibold text-white mb-4">Statistics</h2>
            <div className="space-y-3">
              <div className="flex justify-between">
                <span className="text-gray-400">Constituencies</span>
                <span className="text-white font-mono">
                  {uniqueWinners.length}
                  {uniqueWinners.length !== 234 && (
                    <span className="text-yellow-400 text-xs ml-1">({uniqueWinners.length}/234)</span>
                  )}
                </span>
              </div>
              <div className="flex justify-between">
                <span className="text-gray-400">Districts</span>
                <span className="text-white font-mono">
                  {uniqueDistricts}
                  {uniqueDistricts !== 38 && (
                    <span className="text-yellow-400 text-xs ml-1">({uniqueDistricts}/38)</span>
                  )}
                </span>
              </div>
              <div className="flex justify-between">
                <span className="text-gray-400">Total Sources</span>
                <span className="text-white font-mono">
                  {winners.reduce((sum, w) => sum + w.source_count, 0)}
                </span>
              </div>
              <div className="flex justify-between">
                <span className="text-gray-400">Potential Flips</span>
                <span className="text-white font-mono">
                  {winners.filter(w => w.is_flip).length}
                </span>
              </div>
            </div>
          </div>

          {/* Legend */}
          <MapLegend />
        </div>

        {/* Map */}
        <div className="lg:col-span-3 h-[600px] lg:h-[calc(100vh-200px)]">
          {loading ? (
            <MapLoading />
          ) : error ? (
            <div className="h-full bg-bg-card rounded-xl flex items-center justify-center p-8">
              <div className="text-center max-w-2xl">
                <div className="text-red-400 text-lg font-semibold mb-4">{error}</div>
                <div className="text-gray-400 text-sm space-y-2 text-left bg-gray-800/50 rounded-lg p-4 mt-4">
                  <p className="font-semibold text-gray-300 mb-2">Troubleshooting Steps:</p>
                  <ol className="list-decimal list-inside space-y-1 text-xs">
                    <li>Check that <code className="bg-gray-700 px-1 rounded">frontend/.env.local</code> exists with your Supabase credentials</li>
                    <li>Verify credentials in Supabase Dashboard → Settings → API</li>
                    <li>Ensure <code className="bg-gray-700 px-1 rounded">constituency_predictions</code> table exists (run <code className="bg-gray-700 px-1 rounded">schema.sql</code> in Supabase SQL Editor)</li>
                    <li>Check browser console (F12) for detailed error messages</li>
                    <li>Restart dev server after creating/modifying <code className="bg-gray-700 px-1 rounded">.env.local</code></li>
                  </ol>
                </div>
              </div>
            </div>
          ) : (
            <ElectionMap
              winners={uniqueWinners}
              selectedAlliance={selectedAlliance}
              onConstituencyClick={setSelectedConstituency}
            />
          )}
        </div>
      </div>

      {/* Selected Constituency Detail */}
      {selectedConstituency && (
        <div className="fixed bottom-4 right-4 bg-bg-card rounded-xl p-6 shadow-2xl max-w-sm">
          <button
            onClick={() => setSelectedConstituency(null)}
            className="absolute top-2 right-2 text-gray-400 hover:text-white"
          >
            &times;
          </button>
          <h3 className="text-xl font-bold text-white">
            {selectedConstituency.constituency_name}
          </h3>
          <p className="text-gray-400 text-sm">{selectedConstituency.district} District</p>
          
          <div className="mt-4 space-y-2">
            <div className="flex justify-between">
              <span className="text-gray-400">Predicted Winner</span>
              <span 
                className="px-2 py-0.5 rounded text-xs font-medium"
                style={{ 
                  backgroundColor: getAllianceColor(selectedConstituency.predicted_alliance),
                  color: selectedConstituency.predicted_alliance.includes('TVK') ? '#000' : '#fff'
                }}
              >
                {selectedConstituency.predicted_alliance.replace('_', ' ')}
              </span>
            </div>
            {selectedConstituency.incumbent_2021 && (
              <div className="flex justify-between">
                <span className="text-gray-400">2021 Winner</span>
                <span className="text-white">{selectedConstituency.incumbent_2021.replace('_', ' ')}</span>
              </div>
            )}
            {selectedConstituency.is_flip && (
              <div className="bg-yellow-900/30 text-yellow-300 px-2 py-1 rounded text-xs text-center">
                Potential Flip
              </div>
            )}
            <div className="flex justify-between">
              <span className="text-gray-400">Sentiment Score</span>
              <span className={
                selectedConstituency.sentiment_score > 0 ? 'text-green-400' : 
                selectedConstituency.sentiment_score < 0 ? 'text-red-400' : 'text-gray-400'
              }>
                {selectedConstituency.sentiment_score > 0 ? '+' : ''}
                {(selectedConstituency.sentiment_score * 100).toFixed(1)}%
              </span>
            </div>
            <div className="flex justify-between">
              <span className="text-gray-400">Confidence</span>
              <span className="text-white">
                {(selectedConstituency.confidence_weight * 100).toFixed(0)}%
              </span>
            </div>
            <div className="flex justify-between">
              <span className="text-gray-400">Data Sources</span>
              <span className="text-white">{selectedConstituency.source_count}</span>
            </div>
          </div>
        </div>
      )}

      {/* Footer */}
      <footer className="mt-8 text-center text-gray-500 text-sm">
        <p>
          PollPulse TN - Sentiment-based election predictions using AI/ML
        </p>
        <p className="mt-1">
          Data from YouTube, News Sources | Model: XLM-RoBERTa
        </p>
      </footer>
    </main>
  )
}
