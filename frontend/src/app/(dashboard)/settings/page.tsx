'use client'

import { AppShell } from '@/components/layout/AppShell'
import { SettingsForm } from './components/SettingsForm'
import { useSettings } from '@/lib/hooks/use-settings'
import { Button } from '@/components/ui/button'
import { Card, CardDescription, CardHeader, CardTitle } from '@/components/ui/card'
import { Badge } from '@/components/ui/badge'
import { RefreshCw, Key, Cloud, ChevronRight } from 'lucide-react'
import { useTranslation } from '@/lib/hooks/use-translation'
import { useS3Status } from '@/lib/hooks/use-s3-config'
import Link from 'next/link'

export default function SettingsPage() {
  const { t } = useTranslation()
  const { refetch } = useSettings()
  const { data: s3Status } = useS3Status()

  return (
    <AppShell>
      <div className="flex-1 overflow-y-auto">
        <div className="p-6">
          <div className="max-w-4xl">
            <div className="flex items-center gap-4 mb-6">
              <h1 className="text-2xl font-bold">{t.navigation.settings}</h1>
              <Button variant="outline" size="sm" onClick={() => refetch()}>
                <RefreshCw className="h-4 w-4" />
              </Button>
            </div>

            {/* API Keys Link Card */}
            <Card className="mb-6">
              <Link href="/settings/api-keys" className="block">
                <CardHeader className="pb-3">
                  <div className="flex items-center justify-between">
                    <div className="flex items-center gap-3">
                      <div className="flex h-10 w-10 items-center justify-center rounded-lg bg-primary/10">
                        <Key className="h-5 w-5 text-primary" />
                      </div>
                      <div>
                        <CardTitle className="text-base">{t.apiKeys.title}</CardTitle>
                        <CardDescription className="text-sm">
                          {t.apiKeys.description}
                        </CardDescription>
                      </div>
                    </div>
                    <ChevronRight className="h-5 w-5 text-muted-foreground" />
                  </div>
                </CardHeader>
              </Link>
            </Card>

            <SettingsForm />

            {/* S3 Storage Settings Link */}
            <div className="mt-6">
              <Link href="/settings/s3">
                <Card className="cursor-pointer hover:bg-muted/50 transition-colors">
                  <CardHeader>
                    <div className="flex items-center justify-between">
                      <div className="flex items-center gap-3">
                        <Cloud className="h-5 w-5" />
                        <div>
                          <CardTitle className="text-lg">S3 Storage</CardTitle>
                          <CardDescription>
                            Configure cloud storage for files and podcasts
                          </CardDescription>
                        </div>
                      </div>
                      <div className="flex items-center gap-2">
                        {s3Status?.configured ? (
                          <Badge variant="default">Configured</Badge>
                        ) : (
                          <Badge variant="outline">Not Configured</Badge>
                        )}
                        <ChevronRight className="h-5 w-5 text-muted-foreground" />
                      </div>
                    </div>
                  </CardHeader>
                </Card>
              </Link>
            </div>
          </div>
        </div>
      </div>
    </AppShell>
  )
}
